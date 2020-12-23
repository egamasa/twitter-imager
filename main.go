package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"

	paralleldl "github.com/yyoshiki41/parallel-dl"
)

var tmpDir = "/tmp/images"

var projectID = os.Getenv("PROJECT_ID")
var secretName = os.Getenv("SECRET_NAME")
var secretVersion = os.Getenv("SECRET_VERSION")
var regionName = os.Getenv("REGION_NAME")
var endpoint = os.Getenv("ENDPOINT_URL")
var bucketName = os.Getenv("BUCKET_NAME")

type JSON struct {
	Timeline []*Tweet `json:"timeline"`
	Favorite []*Tweet `json:"favorites"`
}

type Tweet struct {
	User        User   `json:"user"`
	ExtEntities Entity `json:"extended_entities"`
	RT          RT     `json:"retweeted_status"`
}

type RT struct {
	ID          int    `json:"id"`
	User        User   `json:"user"`
	ExtEntities Entity `json:"extended_entities"`
}

type Entity struct {
	Media []*Media `json:"media"`
}

type User struct {
	ImageURL string `json:"profile_image_url_https"`
}

type Media struct {
	URL  string `json:"media_url_https"`
	Type string `json:"type"`
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

type File struct {
	Path string `json:"path"`
	Name string `json:"name"`
}

func getSecret() (string, string, error) {
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: "projects/" + projectID + "/secrets/" + secretName + "/versions/latest",
	}
	result, err := client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		return "", "", err
	}
	secretString := result.Payload.Data
	res := make(map[string]interface{})
	if err := json.Unmarshal([]byte(secretString), &res); err != nil {
		return "", "", err
	}
	return res["ACCESS_KEY_ID"].(string), res["SECRET_ACCESS_KEY"].(string), nil
}

func extractImageURL(tweets []*Tweet) []string {
	images := make([]string, 0)
	for _, tweet := range tweets {
		if tweet.RT.ID == 0 {
			images = append(images, tweet.User.ImageURL)
			if len(tweet.ExtEntities.Media) != 0 {
				for _, media := range tweet.ExtEntities.Media {
					if media.Type != "video" {
						images = append(images, media.URL)
					}
				}
			}
		} else {
			images = append(images, tweet.RT.User.ImageURL)
			if len(tweet.RT.ExtEntities.Media) != 0 {
				for _, media := range tweet.RT.ExtEntities.Media {
					if media.Type != "video" {
						images = append(images, media.URL)
					}
				}
			}
		}
	}
	return images
}

func removeDuplicate(list []string) []string {
	m := make(map[string]struct{})
	newList := make([]string, 0)
	for _, element := range list {
		if _, isDup := m[element]; !isDup {
			m[element] = struct{}{}
			newList = append(newList, element)
		}
	}
	return newList
}

func logJSON(logLevel, message string) string {
	entry := map[string]string{
		"severity": logLevel,
		"message":  message,
	}
	bytes, _ := json.Marshal(entry)
	return string(bytes)
}

func main(jsonPath string, jsonName string) {
	path := aws.String(filepath.Join(jsonPath, jsonName))

	bucket := aws.String(bucketName)
	ak, sk, err := getSecret()
	if err != nil {
		log.Fatal(err)
	}

	s3config := aws.Config{
		Credentials:      credentials.NewStaticCredentialsFromCreds(credentials.Value{AccessKeyID: ak, SecretAccessKey: sk}),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(regionName),
		S3ForcePathStyle: aws.Bool(true),
	}

	sess := s3.New(session.New(), &s3config)

	obj, _ := sess.GetObject(&s3.GetObjectInput{
		Bucket: bucket,
		Key:    path,
	})
	defer obj.Body.Close()

	source := new(JSON)
	if err := json.NewDecoder(obj.Body).Decode(&source); err != nil {
		log.Fatal(err)
	}

	downImages := make([]string, 0)
	downImages = append(downImages, extractImageURL(source.Timeline)...)
	downImages = append(downImages, extractImageURL(source.Favorite)...)
	downImages = removeDuplicate(downImages)
	fmt.Println(logJSON("INFO", fmt.Sprintf("[TwitterImager] Extracted %d URLs.", len(downImages))))

	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		os.Mkdir(tmpDir, 0777)
	}

	options := &paralleldl.Options{
		Output:           tmpDir,
		MaxConcurrents:   4,
		MaxErrorRequests: 1000,
		MaxAttempts:      5,
	}
	client, err := paralleldl.New(options)
	if err != nil {
		log.Fatal(err)
	}
	errCnt := client.Download(downImages)
	fmt.Println(logJSON("INFO", fmt.Sprintf("[TwitterImager] Downloaded images. (Error: %d)", errCnt)))

	upImages, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		log.Fatal(err)
	}

	date := filepath.Base(jsonName[:len(jsonName)-len(filepath.Ext(jsonName))])
	saveDir := filepath.Join(jsonPath, "images_"+filepath.Base(date))
	uploadCount := 0
	for _, image := range upImages {
		file, err := os.Open(filepath.Join(tmpDir, image.Name()))
		if err != nil {
			log.Println(err)
			continue
		}
		_, err = sess.PutObject(&s3.PutObjectInput{
			Body:   file,
			Bucket: bucket,
			Key:    aws.String(filepath.Join(saveDir, image.Name())),
		})
		if err != nil {
			log.Println(err)
			continue
		}
		uploadCount = uploadCount + 1
	}
	fmt.Println(logJSON("INFO", fmt.Sprintf("[TwitterImager] Saved %d images to S3.", uploadCount)))
}

func GetPubSub(ctx context.Context, m PubSubMessage) error {
	var f File
	err := json.Unmarshal(m.Data, &f)
	if err != nil {
		log.Fatal(err)
	}
	main(f.Path, f.Name)
	return nil
}
