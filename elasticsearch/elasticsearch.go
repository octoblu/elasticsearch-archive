package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

// Client interfaces with elasticsearch over REST
// and is capable of taking snapshots, provided the
// snapshot repository is already setup
type Client struct {
	uri, repository string
}

// New constructs a new Elasticsearch instance
func New(uri, repository string) *Client {
	return &Client{uri, repository}
}

// Snapshot creates an ES snapshot for all indexes that contain
// the expression as a substring and creates a new snapshot called
// auto-snapshot-<expression>
func (client *Client) Snapshot(expression string) error {
	request, err := client.buildRequest(expression)
	if err != nil {
		return err
	}

	httpClient := &http.Client{}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != 200 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return fmt.Errorf("Error parsing body of non 200 response on snapshot create: %v, %v", response.StatusCode, err.Error())
		}

		return fmt.Errorf("Received non 200 status code creating snapshot: %v\n%v", response.StatusCode, string(body))
	}

	return nil
}

func (client *Client) buildRequest(expression string) (*http.Request, error) {
	snapshotURL := client.snapshotURL(expression)
	snapshotBody, err := client.snapshotBody(expression)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest("PUT", snapshotURL, snapshotBody)
	if err != nil {
		return nil, err
	}

	request.Header.Add("content-type", "application/json")
	return request, nil
}

func (client *Client) getIndices(expression string) ([]string, error) {
	type Record struct {
		Index string `json:"index"`
	}
	var indices []string
	var records []Record

	listIndexesURI := fmt.Sprintf("%v/_cat/indices", client.uri)
	response, err := http.Get(listIndexesURI)
	if err != nil {
		return indices, err
	}

	if response.StatusCode != 200 {
		return indices, fmt.Errorf("Received non 200 status code while retrieving indices: %v", response.StatusCode)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return indices, err
	}

	err = json.Unmarshal(body, &records)
	if err != nil {
		return indices, err
	}

	for _, record := range records {
		index := record.Index
		if strings.Contains(index, expression) {
			indices = append(indices, record.Index)
		}
	}
	return indices, nil
}

func (client *Client) snapshotBody(expression string) (io.Reader, error) {
	type Body struct {
		Indices string `json:"indices"`
	}

	indices, err := client.getIndices(expression)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(Body{Indices: strings.Join(indices, ",")})
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(body), nil
}

func (client *Client) snapshotURL(expression string) string {
	return fmt.Sprintf("%v/_snapshot/%v/auto-snapshot-%v", client.uri, client.repository, expression)
}
