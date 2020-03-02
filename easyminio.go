// Package easyminio wrapper around https://github.com/minio/minio-go for easier use
package easyminio

import (
	"fmt"
	"io"
	"net/url"
	netUrl "net/url"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v6"
)

// S3Service service to easily interface with s3
type S3Service struct {
	s3Client       *minio.Client
	lifeCycleRules string
	bucketName     string
	urlValues      url.Values
}

// NewS3Service creates a new instace of the s3 service using the provided details
func NewS3Service(url, accessKey, accessSecret, bucketName string) (*S3Service, error) {
	s3Client, err := minio.New(url, accessKey, accessSecret, true)
	if err != nil {
		return nil, err
	}

	exists, err := s3Client.BucketExists(bucketName)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("s3 bucket (%s) doesn't exist", bucketName)
	}

	urlValues := make(netUrl.Values)
	urlValues.Set("response-content-disposition", "inline")

	return &S3Service{
		s3Client:       s3Client,
		lifeCycleRules: "",
		bucketName:     bucketName,
		urlValues:      urlValues,
	}, nil
}

// AddLifeCycleRule adds a lifecycle rule to the remote path
// note: ruleId must be unique
func (s *S3Service) AddLifeCycleRule(ruleID, folderPath string, daysToExpiry int) error {
	if !strings.HasSuffix(folderPath, "/") {
		folderPath += "/"
	}

	lifeCycleString := fmt.Sprintf(
		`<LifecycleConfiguration><Rule><ID>%s</ID><Prefix>%s</Prefix><Status>Enabled</Status><Expiration><Days>%d</Days></Expiration></Rule></LifecycleConfiguration>`,
		ruleID, folderPath, daysToExpiry)

	return s.s3Client.SetBucketLifecycle(s.bucketName, lifeCycleString)
}

// UploadJSONFile uploads a json file from the reader to the specified path
func (s *S3Service) UploadJSONFile(path string, data io.Reader) error {
	_, err := s.s3Client.PutObject(s.bucketName, path, data, -1, minio.PutObjectOptions{ContentType: "application/json"})
	return err
}

// GetFileURL generates a link to the file at the given path
// that expires after the specified duration
func (s *S3Service) GetFileURL(path string, expiration time.Duration) (*url.URL, error) {
	return s.s3Client.PresignedGetObject(s.bucketName, path, expiration, s.urlValues)
}

// UploadJSONFileWithLink uploads a json file and returns a public link to the file
// that expires after the specified duration
func (s *S3Service) UploadJSONFileWithLink(path string, data io.Reader, expiration time.Duration) (*url.URL, error) {
	_, err := s.s3Client.PutObject(s.bucketName, path, data, -1, minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return nil, err
	}

	return s.s3Client.PresignedGetObject(s.bucketName, path, 24*time.Hour, s.urlValues)
}

// DownloadDirectory concurrently downloads the remote s3 directory path
// to the local file system at the specified location
func (s *S3Service) DownloadDirectory(path, localPath string) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := s.s3Client.ListObjectsV2(s.bucketName, path, true, doneCh)

	wg := sync.WaitGroup{}
	errCh := make(chan error)

	for obj := range objectCh {
		if obj.Err != nil {
			return obj.Err
		}

		wg.Add(1)

		go func(obj minio.ObjectInfo) {
			if strings.HasSuffix(obj.Key, "/") { // don't try to download directory as a file
				wg.Done()
				return
			}

			fileName := strings.TrimPrefix(obj.Key, path+"/")

			err := s.DownloadFile(obj.Key, localPath+"/"+fileName)
			if err != nil {
				errCh <- err
			}

			wg.Done()
		}(obj)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	errs := []error{}
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to download files from s3: %v", errs)
	}

	return nil
}

// DownloadFile downloads the file at path to the specified local path
func (s *S3Service) DownloadFile(path, localPath string) error {
	return s.s3Client.FGetObject(s.bucketName, path, localPath, minio.GetObjectOptions{})
}
