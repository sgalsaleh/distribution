package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	"golang.org/x/sync/errgroup"
)

func emit(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

// GCOpts contains options for garbage collector
type GCOpts struct {
	DryRun         bool
	RemoveUntagged bool
}

// ManifestDel contains manifest structure which will be deleted
type ManifestDel struct {
	Name   string
	Digest digest.Digest
	Tags   []string
}

// MarkAndSweep performs a mark and sweep of registry data
func MarkAndSweep(ctx context.Context, storageDriver driver.StorageDriver, registry distribution.Namespace, opts GCOpts) error {
	repositoryEnumerator, ok := registry.(distribution.RepositoryEnumerator)
	if !ok {
		return fmt.Errorf("unable to convert Namespace to RepositoryEnumerator")
	}

	// process in parallel
	const concurrencyLimit = 100
	var markSetMtx sync.Mutex
	var deleteLayerSetMtx sync.Mutex
	var manifestArrMtx sync.Mutex
	repoG, _ := errgroup.WithContext(context.Background())
	repoG.SetLimit(concurrencyLimit)
	manifestG, _ := errgroup.WithContext(context.Background())
	manifestG.SetLimit(concurrencyLimit)

	// mark
	markSet := make(map[digest.Digest]struct{})
	deleteLayerSet := make(map[string][]digest.Digest)
	manifestArr := make([]ManifestDel, 0)
	err := repositoryEnumerator.Enumerate(ctx, func(repoName string) error {
		func(repoName string) {
			repoG.Go(func() error {
				emit(repoName)

				var err error
				named, err := reference.WithName(repoName)
				if err != nil {
					emit("failed to parse repo name %s: %v", repoName, err)
					return nil
				}
				repository, err := registry.Repository(ctx, named)
				if err != nil {
					emit("failed to construct repository: %v", err)
					return nil
				}

				manifestService, err := repository.Manifests(ctx)
				if err != nil {
					emit("failed to construct manifest service: %v", err)
					return nil
				}

				manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
				if !ok {
					emit("unable to convert ManifestService into ManifestEnumerator")
					return nil
				}

				err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
					func(ctx context.Context, dgst digest.Digest) {
						manifestG.Go(func() error {
							if opts.RemoveUntagged {
								// fetch all tags where this manifest is the latest one
								tags, err := repository.Tags(ctx).Lookup(ctx, distribution.Descriptor{Digest: dgst})
								if err != nil {
									emit("failed to retrieve tags for digest %v: %v", dgst, err)
									return nil
								}
								if len(tags) == 0 {
									// fetch all tags from repository
									// all of these tags could contain manifest in history
									// which means that we need check (and delete) those references when deleting manifest
									allTags, err := repository.Tags(ctx).All(ctx)
									if err != nil {
										if _, ok := err.(distribution.ErrRepositoryUnknown); ok {
											emit("manifest tags path of repository %s does not exist", repoName)
											return nil
										}
										emit("failed to retrieve tags %v", err)
										return nil
									}
									manifestArrMtx.Lock()
									manifestArr = append(manifestArr, ManifestDel{Name: repoName, Digest: dgst, Tags: allTags})
									manifestArrMtx.Unlock()
									return nil
								}
							}
							// Mark the manifest's blob
							emit("%s: marking manifest %s ", repoName, dgst)
							markSetMtx.Lock()
							markSet[dgst] = struct{}{}
							markSetMtx.Unlock()

							return markManifestReferences(dgst, manifestService, ctx, func(d digest.Digest) bool {
								markSetMtx.Lock()
								defer markSetMtx.Unlock()
								_, marked := markSet[d]
								if !marked {
									markSet[d] = struct{}{}
									emit("%s: marking blob %s", repoName, d)
								}
								return marked
							})
						})
					}(ctx, dgst)

					return nil
				})

				if err != nil {
					// In certain situations such as unfinished uploads, deleting all
					// tags in S3 or removing the _manifests folder manually, this
					// error may be of type PathNotFound.
					//
					// In these cases we can continue marking other manifests safely.
					if _, ok := err.(driver.PathNotFoundError); !ok {
						emit("path not found: %v", err)
						return nil
					}
				}
				blobService := repository.Blobs(ctx)
				layerEnumerator, ok := blobService.(distribution.ManifestEnumerator)
				if !ok {
					emit("unable to convert BlobService into ManifestEnumerator")
					return nil
				}

				var deleteLayers []digest.Digest
				err = layerEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
					markSetMtx.Lock()
					defer markSetMtx.Unlock()
					if _, ok := markSet[dgst]; !ok {
						deleteLayers = append(deleteLayers, dgst)
					}
					return nil
				})
				if len(deleteLayers) > 0 {
					deleteLayerSetMtx.Lock()
					deleteLayerSet[repoName] = deleteLayers
					deleteLayerSetMtx.Unlock()
				}
				if err != nil {
					emit("error enumerating layers: %v", err)
				}
				return nil
			})
		}(repoName)

		return nil
	})
	if err != nil {
		emit("failed to mark: %v", err)
	}
	if err := repoG.Wait(); err != nil {
		emit("error enumerating repositories: %v", err)
	}
	if err := manifestG.Wait(); err != nil {
		emit("error enumerating manifests: %v", err)
	}

	manifestArr = unmarkReferencedManifest(manifestArr, markSet)

	// sweep
	deleteManifestG, _ := errgroup.WithContext(context.Background())
	deleteManifestG.SetLimit(concurrencyLimit)
	vacuum := NewVacuum(ctx, storageDriver)
	if !opts.DryRun {
		for _, obj := range manifestArr {
			func(obj ManifestDel) {
				deleteManifestG.Go(func() error {
					err = vacuum.RemoveManifest(obj.Name, obj.Digest, obj.Tags)
					if err != nil {
						emit("failed to delete manifest %s: %v", obj.Digest, err)
					}
					return nil
				})
			}(obj)
		}
	}
	if err := deleteManifestG.Wait(); err != nil {
		emit("error deleting manifests: %v", err)
	}

	blobService := registry.Blobs()
	deleteSet := make(map[digest.Digest]struct{})
	err = blobService.Enumerate(ctx, func(dgst digest.Digest) error {
		// check if digest is in markSet. If not, delete it!
		if _, ok := markSet[dgst]; !ok {
			emit("blob eligible for deletion: %s", dgst)
			deleteSet[dgst] = struct{}{}
		}
		return nil
	})
	if err != nil {
		emit("error enumerating blobs: %v", err)
	}
	emit("\n%d blobs marked, %d blobs and %d manifests eligible for deletion", len(markSet), len(deleteSet), len(manifestArr))

	deleteBlobsG, _ := errgroup.WithContext(context.Background())
	deleteBlobsG.SetLimit(concurrencyLimit)
	for dgst := range deleteSet {
		func(dgst digest.Digest) {
			deleteBlobsG.Go(func() error {
				emit("deleting blob: %s", dgst)
				if opts.DryRun {
					return nil
				}
				err = vacuum.RemoveBlob(string(dgst))
				if err != nil {
					emit("failed to delete blob %s: %v", dgst, err)
				}
				return nil
			})
		}(dgst)
	}
	if err := deleteBlobsG.Wait(); err != nil {
		emit("error removing blobs: %v", err)
	}

	deleteLayersG, _ := errgroup.WithContext(context.Background())
	deleteLayersG.SetLimit(concurrencyLimit)
	for repo, dgsts := range deleteLayerSet {
		for _, dgst := range dgsts {
			func(dgst digest.Digest) {
				deleteLayersG.Go(func() error {
					emit("%s: layer link eligible for deletion: %s", repo, dgst)
					if opts.DryRun {
						return nil
					}
					err = vacuum.RemoveLayer(repo, dgst)
					if err != nil {
						emit("failed to delete layer link %s of repo %s: %v", dgst, repo, err)
					}
					return nil
				})
			}(dgst)
		}
	}
	if err := deleteLayersG.Wait(); err != nil {
		emit("error deleting layers: %v", err)
	}

	return err
}

// unmarkReferencedManifest filters out manifest present in markSet
func unmarkReferencedManifest(manifestArr []ManifestDel, markSet map[digest.Digest]struct{}) []ManifestDel {
	filtered := make([]ManifestDel, 0)
	for _, obj := range manifestArr {
		if _, ok := markSet[obj.Digest]; !ok {
			emit("manifest eligible for deletion: %s", obj)
			filtered = append(filtered, obj)
		}
	}
	return filtered
}

// markManifestReferences marks the manifest references
func markManifestReferences(dgst digest.Digest, manifestService distribution.ManifestService, ctx context.Context, ingester func(digest.Digest) bool) error {
	manifest, err := manifestService.Get(ctx, dgst)
	if err != nil {
		return fmt.Errorf("failed to retrieve manifest for digest %v: %v", dgst, err)
	}

	descriptors := manifest.References()
	for _, descriptor := range descriptors {

		// do not visit references if already marked
		if ingester(descriptor.Digest) {
			continue
		}

		if ok, _ := manifestService.Exists(ctx, descriptor.Digest); ok {
			err := markManifestReferences(descriptor.Digest, manifestService, ctx, ingester)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
