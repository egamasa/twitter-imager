package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	// go get github.com/yyoshiki41/parallel-dl
	paralleldl "github.com/yyoshiki41/parallel-dl"
)

type JSON struct {
	Timeline []*Tweet `json:"timeline"`
	Favorite []*Tweet `json:"favorites"`
}

type Tweet struct {
	User        User   `json:"user"`
	ExtEntities Entity `json:"extended_entities"`
	RT          RT     `json:"retweeted_status"`
	// QT          QT     `json:"quoted_status"`
}

type RT struct {
	ID          int    `json:"id"`
	User        User   `json:"user"`
	ExtEntities Entity `json:"extended_entities"`
}

// type QT struct {
// 	ID          int    `json:"id"`
// 	User        User   `json:"user"`
// 	ExtEntities Entity `json:"extended_entities"`
// }

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

func main() {
	file, err := ioutil.ReadFile("test.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	source := new(JSON)
	err = json.Unmarshal(file, &source)

	images := make([]string, 0)
	images = append(images, extractImageURL(source.Timeline)...)
	images = append(images, extractImageURL(source.Favorite)...)
	fmt.Println(len(images))
	images = removeDuplicate(images)
	fmt.Println(len(images))
	options := &paralleldl.Options{
		Output:           "./images/",
		MaxConcurrents:   4,
		MaxErrorRequests: 1000,
		MaxAttempts:      5,
	}
	client, err := paralleldl.New(options)
	if err != nil {
		log.Fatal(err)
	}
	errCnt := client.Download(images)
	fmt.Println(errCnt)
}
