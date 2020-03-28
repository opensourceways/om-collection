package main

import (
	"net/http"
	"fmt"
	"strconv"
	"encoding/json"

	"github.com/huaweicloud/golangsdk"
	"github.com/huaweicloud/golangsdk/openstack"
	"github.com/huaweicloud/golangsdk/openstack/ces/v1/metricdata"
)

type Config struct {
	AccessKey        string
	SecretKey        string
	DomainID         string
	DomainName       string
	EndpointType     string
	IdentityEndpoint string
	Insecure         bool
	Password         string
	Region           string
	TenantID         string
	TenantName       string
	Token            string
	Username         string
	UserID           string

	HwClient *golangsdk.ProviderClient
}

func buildClient(c *Config) error {
	err := fmt.Errorf("Must config token or aksk or username password to be authorized")

	if c.AccessKey != "" && c.SecretKey != "" {
		err = buildClientByAKSK(c)
	} else if c.Password != "" && (c.Username != "" || c.UserID != "") {
		err = buildClientByPassword(c)
	}

	if err != nil {
		return err
	}

	return nil
}

func buildClientByPassword(c *Config) error {
	var pao, dao golangsdk.AuthOptions

	pao = golangsdk.AuthOptions{
		DomainID:   c.DomainID,
		DomainName: c.DomainName,
		TenantID:   c.TenantID,
		TenantName: c.TenantName,
	}

	dao = golangsdk.AuthOptions{
		DomainID:   c.DomainID,
		DomainName: c.DomainName,
	}

	for _, ao := range []*golangsdk.AuthOptions{&pao, &dao} {
		ao.IdentityEndpoint = c.IdentityEndpoint
		ao.Password = c.Password
		ao.Username = c.Username
		ao.UserID = c.UserID
	}

	return genClients(c, pao, dao)
}

func buildClientByAKSK(c *Config) error {
	var pao, dao golangsdk.AKSKAuthOptions

	pao = golangsdk.AKSKAuthOptions{
		ProjectName: c.TenantName,
		ProjectId:   c.TenantID,
	}

	dao = golangsdk.AKSKAuthOptions{
		DomainID: c.DomainID,
		Domain:   c.DomainName,
	}

	for _, ao := range []*golangsdk.AKSKAuthOptions{&pao, &dao} {
		ao.IdentityEndpoint = c.IdentityEndpoint
		ao.AccessKey = c.AccessKey
		ao.SecretKey = c.SecretKey
	}
	return genClients(c, pao, dao)
}

func genClients(c *Config, pao, dao golangsdk.AuthOptionsProvider) error {
	client, err := genClient(c, pao)
	if err != nil {
		return err
	}
	c.HwClient = client
	return err
}

func genClient(c *Config, ao golangsdk.AuthOptionsProvider) (*golangsdk.ProviderClient, error) {
	client, err := openstack.NewClient(ao.GetIdentityEndpoint())
	if err != nil {
		return nil, err
	}

	client.HTTPClient = http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if client.AKSKAuthOptions.AccessKey != "" {
				golangsdk.ReSign(req, golangsdk.SignOptions{
					AccessKey: client.AKSKAuthOptions.AccessKey,
					SecretKey: client.AKSKAuthOptions.SecretKey,
				})
			}
			return nil
		},
	}

	err = openstack.Authenticate(client, ao)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func InitConfig()(*Config, error)  {
	configOptions := Config{
		IdentityEndpoint: "",
		TenantName:      "",
		AccessKey:       "",
		SecretKey:       "",
		DomainName:      "",
		Username:        "",
		Region:          "",
		Password:        "",
		Insecure:        true,
	}

	err := buildClient(&configOptions)
	if err != nil {
		fmt.Println("Failed to build client: ", err)
		return nil, err
	}

	return &configOptions, err
}

func getCESClient(c *Config)(*golangsdk.ServiceClient, error)  {
	client, clientErr := openstack.NewCESClient(c.HwClient, golangsdk.EndpointOpts{
		Region: c.Region,
	})

	if clientErr != nil {
		fmt.Println("Failed to get the NewCESV1 client: ", clientErr)
		return nil, clientErr
	}
	return client, nil
}


func (c *Config) getBatchMetricData(metrics *[]metricdata.Metric,
	from string, to string) (*[]metricdata.MetricData, error){

	ifrom, _ := strconv.ParseInt(from, 10, 64)
	ito, _ := strconv.ParseInt(to, 10, 64)
	options := metricdata.BatchQueryOpts {
		Metrics: *metrics,
		From: ifrom,
		To: ito,
		Period: "1",
		Filter: "average",
	}

	client, err := getCESClient(c)
	if err != nil {
		fmt.Println("Failed to get ces client: ", err)
		return nil, err
	}

	v, err := metricdata.BatchQuery(client, options).ExtractMetricDatas()
	if err != nil {
		fmt.Println("Failed to get metricdata: ", err)
		return nil, err
	}

	return &v, nil
}

