/*
Copyright 2020 The community Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"strconv"
	"github.com/spf13/cobra"
	"strings"
	"sync"
	"time"

	"encoding/base64"
	"regexp"

	"io/ioutil"
	"context"
	"encoding/json"
	"bytes"
	"io"
	"net/http"
	"net/url"
	"github.com/pkg/errors"

	"github.com/huaweicloud/golangsdk/openstack/ces/v1/metricdata"
)

type OpenEulerStatics struct {
	GiteeToken string
	HWUser string
	HWPassword string
	ShowStar bool
	ShowSubscribe bool
	ShowPR bool
	ShowIssue bool
	Threads int32
}


// StoreSamplesRequest is used for building a JSON request for storing samples
// via the OpenTSDB.
type StoreSamplesRequest struct {
	Metric    string              `json:"metric"`
	Timestamp int64               `json:"timestamp"`
	Value     float64             `json:"value"`
	Tags      map[string]string   `json:"tags"`
}


const (
	putEndpoint     = "/api/put"
	contentTypeJSON = "application/json"
)

var openEulerStatics = &OpenEulerStatics{}
var t int64 = time.Now().Unix()

var mergePRUsers []string
var mergeIssueUser []string

func InitStaticsFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&openEulerStatics.GiteeToken, "giteetoken", "g", "", "the gitee token")
	cmd.Flags().StringVarP(&openEulerStatics.HWUser, "user", "u", "", "the username for huawei cloud")
	cmd.Flags().StringVarP(&openEulerStatics.HWPassword, "password", "p", "", "the password for huawei cloud")
	cmd.Flags().BoolVarP(&openEulerStatics.ShowStar, "showstar", "s", false, "whether show stars count")
	cmd.Flags().BoolVarP(&openEulerStatics.ShowSubscribe, "showsubscribe", "w", false, "whether show subscribe count")
	cmd.Flags().BoolVarP(&openEulerStatics.ShowPR, "showpr", "r", false, "whether show pr count")
	cmd.Flags().BoolVarP(&openEulerStatics.ShowIssue, "showissue", "i", false, "whether show pr count")
	cmd.Flags().Int32VarP(&openEulerStatics.Threads, "threads", "t", 5, "how many threads to perform")
}

func Write(reqs []StoreSamplesRequest) error {
	u, err := url.Parse("http://127.0.0.1:4242")
	if err != nil {
		return err
	}

	u.Path = putEndpoint

	buf, err := json.Marshal(reqs)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(60000) * time.Second)
	defer cancel()

	req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", contentTypeJSON)

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	// API returns status code 204 for successful writes.
	// http://opentsdb.net/docs/build/html/api_http/put.html
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	// API returns status code 400 on error, encoding error details in the
	// response content in JSON.
	buf, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Put data to opentsdb fail:%s.\n", buf)
		return err
	}

	var r map[string]int
	if err := json.Unmarshal(buf, &r); err != nil {
		fmt.Printf("Unmarshal opentsdb fail:%s.\n", buf)
		return err
	}
	fmt.Printf("failed to write %d samples to OpenTSDB, %d succeeded", r["failed"], r["success"])
	return errors.Errorf("failed to write %d samples to OpenTSDB, %d succeeded", r["failed"], r["success"])
	//return nil
}

func buildStaticsCommand() *cobra.Command {
	staticsCommand := &cobra.Command{
		Use:   "statics",
		Short: "show current statics of openEuler",
	}

	showCommand := &cobra.Command{
		Use:   "show",
		Short: "show current statics of openEuler",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, ShowAllStatics())
		},
	}
	InitStaticsFlags(showCommand)
	staticsCommand.AddCommand(showCommand)

	return staticsCommand
}

func ShowAllStatics() error {
	fmt.Printf("now is %s\n", time.Now().String())
	err := ShowStatics("mindspore")
	if err != nil {
		return err
	}
	ShowStatics("src-openeuler")

	if (openEulerStatics.ShowPR) {
		tmpTag := make(map[string]string)
		tmpTag["org"] = "all"
		mergePrUser := writeMetric(t, "gitee_pr_merged_toltal_user_all", float64(len(mergePRUsers)), tmpTag)
		fmt.Printf("Collecting gitee_pr_merged_toltal_user_all = %d\n", len(mergePRUsers))
		Write(mergePrUser)
	}

	if (openEulerStatics.ShowIssue) {
		tmpTag := make(map[string]string)
		tmpTag["org"] = "all"
		issueUsers := writeMetric(t, "gitee_issue_merged_toltal_user_all", float64(len(mergeIssueUser)), tmpTag)
		fmt.Printf("Collecting gitee_issue_merged_toltal_user_all = %d\n", len(mergeIssueUser))
		Write(issueUsers)
	}


	//ShowObsStatics()

	return nil
}

func ShowObsStatics() error {
	//authUrl := "https://iam.cn-north-1.myhwclouds.com/v3"
	//region := "cn-north-1"

	hwCloudHandler, err := InitConfig()
	if err != nil {
		return err
	}

	var metrics []metricdata.Metric
	var dims []metricdata.Dimension
	download_dim := metricdata.Dimension{"bandwidth_id","eed8adb9-7d0f-4059-9f35-4c34127740b3"}

	dims = append(dims, download_dim)
	d := metricdata.Metric{"SYS.VPC", "up_stream", dims}
	metrics = append(metrics, d)

	now := time.Now()
	periodm, _ := time.ParseDuration("-30m")
	from := strconv.FormatInt(int64(now.Add(periodm).UnixNano() / 1e6),10)
	to := strconv.FormatInt(int64(now.UnixNano() / 1e6),10)

	mds, _ := hwCloudHandler.getBatchMetricData(&metrics, from, to)

	var samples []StoreSamplesRequest
	for _, md := range *mds {
		for _, dp := range md.Datapoints {
			var sample StoreSamplesRequest
			sample.Value = dp.Average
			sample.Metric = "huaweicloud_bandwith_up_streamttt"
			sample.Timestamp = int64(dp.Timestamp)
			Tags := make(map[string]string)
			Tags["bandwidth_id"] = md.Dimensions[0].Value
			sample.Tags = Tags
			samples = append(samples, sample)

			fmt.Printf("Collecting ces up stream =%v\n", sample)
			//Tags["bandwidth_ip"] = md.dimensions[0].value
		}
	}

	countAll := len(samples)
	var tmp []StoreSamplesRequest
	for i := 0; i < countAll; i++ {
		if (len(tmp) == 20) {
			Write(tmp)
			tmp = nil
		}
		tmp = append(tmp, samples[i])
	}

	if len(tmp) > 0 {
		Write(tmp)
	}

	return nil
}

func writeMetric(t int64, metricName string, value float64, tags map[string]string) ([]StoreSamplesRequest) {
	var sampleRequests []StoreSamplesRequest
	var ss StoreSamplesRequest
	ss.Timestamp = t
	ss.Metric = metricName
	ss.Value = value
	ss.Tags = tags
	sampleRequests = append(sampleRequests, ss)
	Write(sampleRequests)
	return sampleRequests
}

func setPRMetric(prResults []PullRequest, organization string, t int64)  {
	totalMergePRNum := 0
	var samples []StoreSamplesRequest
	for _,pr := range prResults {
		//tm, _ := time.Parse("2006-01-02T15:04:05+08:00", pr.CreateAt)

		var tmpTag map[string]string
		tmpTag = make(map[string]string)
		tmpTag["org"] = organization
		//tmpTag["RepoName"] = pr.RepoName
		//tmpTag["name"] = strings.Split(pr.RepoName, "/")[1]
		tmpTag["State"] = pr.State

		encodeAuther := base64.StdEncoding.EncodeToString([]byte(pr.Auther))
		reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
		tmpTag["Auther"] = reg.ReplaceAllString(encodeAuther, "")
		tmpTag["Link"] = pr.Link
		//tmpTag["Number"] = string(pr.Number)
		//tmpTag["CreateAt"] = string(tm)

		ss := writeMetric(t, "gitee_pr_num2", float64(1), tmpTag)
		samples = append(samples, ss...)
		fmt.Printf("%s, %s, %d, %s, %s, %s\n", pr.RepoName, pr.CreateAt, pr.Number, pr.Auther, pr.State, pr.Link)
		if pr.State == "merged"{
			totalMergePRNum++
			if !Find(mergePRUsers, pr.Auther) {
				mergePRUsers = append(mergePRUsers, pr.Auther)
			}
		}
	}

	var mergeTag map[string]string
	mergeTag = make(map[string]string)
	mergeTag["org"] = organization
	mergePr := writeMetric(t, "gitee_pr_merged_toltal_num2", float64(totalMergePRNum), mergeTag)
	mergePrUser := writeMetric(t, "gitee_pr_merged_toltal_user_for_org", float64(len(mergePRUsers)), mergeTag)
	Write(mergePr)
	Write(mergePrUser)

	countAll := len(samples)
	var tmp []StoreSamplesRequest
	for i := 0; i < countAll; i++ {
		if (len(tmp) == 20) {
			Write(tmp)
			tmp = nil
		}
		tmp = append(tmp, samples[i])
	}

	Write(tmp)
}

func setIssueMetric(issueResults []IssueRequest, organization string, t int64)  {
	totalMergeIssueNum := 0
	var samples []StoreSamplesRequest

	for _,issue := range issueResults {
		//tm, _ := time.Parse("2006-01-02T15:04:05+08:00", issue.CreateAt)

		var tmpTag map[string]string
		tmpTag = make(map[string]string)
		tmpTag["org"] = organization
		//tmpTag["RepoName"] = issue.RepoName
		//tmpTag["name"] = strings.Split(issue.RepoName, "/")[1]
		tmpTag["State"] = issue.State

		encodeAuther := base64.RawStdEncoding.EncodeToString([]byte(issue.Auther))
		//fmt.Println(encoded)
		reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
		tmpTag["Auther"] = reg.ReplaceAllString(encodeAuther, "")
		tmpTag["Link"] = issue.Link
		//tmpTag["Number"] = issue.Number
		//tmpTag["CreateAt"] = string(tm)

		ss := writeMetric(t, "gitee_issue_num2", float64(1), tmpTag)
		samples = append(samples, ss...)
		fmt.Printf("%s, %s, %s, %s, %s\n", issue.RepoName, issue.Number, issue.Auther, issue.State, issue.Link)
		//if issue.State == "merged"{
		totalMergeIssueNum++
		if !Find(mergeIssueUser, issue.Auther) {
			mergeIssueUser = append(mergeIssueUser, issue.Auther)
		}
		//}
	}

	file, _ := json.MarshalIndent(samples, "", " ")
	_ = ioutil.WriteFile("issues.json", file, 0644)

	var mergeTag map[string]string
	mergeTag = make(map[string]string)
	mergeTag["org"] = organization
	mergeIssue := writeMetric(t, "gitee_issue_merged_toltal_num2", float64(totalMergeIssueNum), mergeTag)
	issueUsers := writeMetric(t, "gitee_issue_merged_toltal_user_for_org", float64(len(mergeIssueUser)), mergeTag)
	Write(mergeIssue)
	Write(issueUsers)

	countAll := len(samples)
	var tmp []StoreSamplesRequest
	for i := 0; i < countAll; i++ {
		if (len(tmp) == 20) {
			Write(tmp)
			tmp = nil
		}
		tmp = append(tmp, samples[i])
	}

	Write(tmp)
}


func ShowStatics(organization string) error {
	var collectingwg sync.WaitGroup
	var endwg sync.WaitGroup
	var totalUsers []string
	var totalSubscribeUsers []string
	var totalProjects []string
	resultChannel := make(chan string, 50)
	subscribeChannel := make(chan string, 50)
	projectChannel := make(chan string, 50)
	prChannel := make(chan PullRequest, 50)
	issueChannel := make(chan IssueRequest, 50)
	prResults := []PullRequest{}
	issueResults := []IssueRequest{}
	// Collecting contributing information from openeuler organization
	giteeHandler := NewGiteeHandler(openEulerStatics.GiteeToken)
	// Running 5 workers to collect the projects status
	size := giteeHandler.CollectRepoPageCount(100, "mindspore")
	if size <= 0 {
		return fmt.Errorf("can't get any projects in enterprise 'mindspore'")
	}

	go func() {
		endwg.Add(1)
		for rs := range projectChannel {
			totalProjects = append(totalProjects, rs)
		}
		endwg.Done()
	}()
	for i := 1; i <= int(openEulerStatics.Threads); i++ {
		collectingwg.Add(1)
		go giteeHandler.CollectRepos(&collectingwg,100, size, i, int(openEulerStatics.Threads) , "mindspore", projectChannel, )
	}

	collectingwg.Wait()
	close(projectChannel)
	endwg.Wait()

	go func() {
		endwg.Add(1)
		for rs := range resultChannel {
			if !Find(totalUsers, rs) {
				totalUsers = append(totalUsers, rs)
			}
		}
		endwg.Done()
	}()

	go func() {
		endwg.Add(1)
		for rs := range subscribeChannel {
			if !Find(totalSubscribeUsers, rs) {
				totalSubscribeUsers = append(totalSubscribeUsers, rs)
			}
		}
		endwg.Done()
	}()

	go func() {
		endwg.Add(1)
		for pr := range prChannel {
			prResults = append(prResults, pr)
		}
		endwg.Done()
	}()

	go func() {
		endwg.Add(1)
		for issue := range issueChannel {
			issueResults = append(issueResults, issue)
		}
		endwg.Done()
	}()

	if (openEulerStatics.ShowStar) {
		for i := 0; i <= len(totalProjects); i+=int(openEulerStatics.Threads) {
			groupwg := sync.WaitGroup{}
			for j := i; j < i+int(openEulerStatics.Threads); j++ {
				if  j < len(totalProjects) && (strings.HasPrefix(totalProjects[j], fmt.Sprintf("%s/", organization))) {
					fmt.Printf("Collecting Star info for project %s\n", totalProjects[j])
					groupwg.Add(1)
					go giteeHandler.ShowRepoStarStatics(&groupwg, strings.Split(totalProjects[j], "/")[0], strings.Split(totalProjects[j], "/")[1], resultChannel)
				}

			}
			groupwg.Wait()
		}
	}

	if (openEulerStatics.ShowSubscribe){
		for i := 0; i <= len(totalProjects); i+=int(openEulerStatics.Threads) {
			groupwg := sync.WaitGroup{}
			for j := i; j < i+int(openEulerStatics.Threads); j++ {

				if  j < len(totalProjects) && (strings.HasPrefix(totalProjects[j], fmt.Sprintf("%s/", organization))) {
					fmt.Printf("Collecting Subsribe info for project %s\n", totalProjects[j])
					groupwg.Add(1)
					go giteeHandler.ShowRepoWatchStatics(&groupwg, strings.Split(totalProjects[j], "/")[0], strings.Split(totalProjects[j], "/")[1], subscribeChannel)
				}
			}
			groupwg.Wait()
		}
	}

	if (openEulerStatics.ShowPR) {
		for i := 0; i <= len(totalProjects); i+=int(openEulerStatics.Threads) {
			groupwg := sync.WaitGroup{}
			for j := i; j < i+int(openEulerStatics.Threads); j++ {

				if  j < len(totalProjects) && (strings.HasPrefix(totalProjects[j], fmt.Sprintf("%s/", organization))) {
					fmt.Printf("Collecting PR info for project %s, j=%d\n", totalProjects[j], j)
					groupwg.Add(1)
					go giteeHandler.ShowRepoPRs(&groupwg, strings.Split(totalProjects[j], "/")[0], strings.Split(totalProjects[j], "/")[1], prChannel)
				}
			}
			groupwg.Wait()
		}
	}

	if (openEulerStatics.ShowIssue) {
		for i := 0; i <= len(totalProjects); i+=int(openEulerStatics.Threads) {
			groupwg := sync.WaitGroup{}
			for j := i; j < i+int(openEulerStatics.Threads); j++ {
				if  j < len(totalProjects) && (strings.HasPrefix(totalProjects[j], fmt.Sprintf("%s/", organization))) {
					fmt.Printf("Collecting Issue info for project %s, j=%d\n", totalProjects[j], j)
					groupwg.Add(1)
					go giteeHandler.ShowRepoIssues(&groupwg, strings.Split(totalProjects[j], "/")[0], strings.Split(totalProjects[j], "/")[1], issueChannel)
				}
			}
			groupwg.Wait()
		}
	}

	close(resultChannel)
	close(subscribeChannel)
	close(prChannel)
	close(issueChannel)
	endwg.Wait()




	if (openEulerStatics.ShowStar) {
		var tmpTag map[string]string
		tmpTag = make(map[string]string)
		tmpTag["org"] = organization
		ss := writeMetric(t, "gitee_all_star_num_for_org", float64(len(totalUsers)), tmpTag)
		Write(ss)
		fmt.Printf("[Result] There are %d users stars %s project \n.", len(totalUsers), organization)
	}
	if (openEulerStatics.ShowSubscribe) {
		var tmpTag map[string]string
		tmpTag = make(map[string]string)
		tmpTag["org"] = organization
		ss := writeMetric(t, "gitee_all_watch_num_for_org", float64(len(totalSubscribeUsers)), tmpTag)
		Write(ss)
		fmt.Printf("[Result] There are %d users subscribe %s project \n.", len(totalSubscribeUsers), organization)
	}
	if (openEulerStatics.ShowPR) {
		fmt.Printf("[Result] The contribution info for  %sis: \n", organization)
		fmt.Printf("Repo, CreateAt, PR Number, Auther, State, Link\n")
		setPRMetric(prResults, organization, t)
	}
	if (openEulerStatics.ShowIssue) {
		fmt.Printf("[Result] The issue info for  %sis: \n", organization)
		fmt.Printf("Repo, CreateAt, PR Number, Auther, State, Link\n")
		setIssueMetric(issueResults, organization, t)
	}

	return nil
}
