/*
Copyright 2019 The openeuler community Authors.

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
	"context"
	"fmt"
	"github.com/antihax/optional"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"strings"

	"gitee.com/openeuler/go-gitee/gitee"
	"golang.org/x/oauth2"
)

var onlyOneSignalHandler = make(chan struct{})
var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

type SimplifiedRepo struct {
	Id        int     `json:"id,omitempty"`
	FullName  *string `json:"full_name,omitempty"`
	Url       *string  `json:"url,omitempty"`
}

type PullRequest struct {
	Id int32
	Auther string
	State string
	RepoName string
	Number int32
	Link string
	CreateAt string
}

type IssueRequest struct {
	Id int32
	Auther string
	State string
	RepoName string
	Number string
	Link string
}

func SetupSignalHandler() (stopCh <-chan struct{}) {
	close(onlyOneSignalHandler) // panics when called twice

	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

type GiteeHandler struct {
	GiteeClient *gitee.APIClient
	Token string
	Context context.Context
}


func NewGiteeHandler(giteeToken string) *GiteeHandler{
	// oauth
	oauthSecret := checkOwnerFlags.GiteeToken
	ctx := context.Background()
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: string(oauthSecret)},
	)

	// configuration
	giteeConf := gitee.NewConfiguration()
	giteeConf.HTTPClient = oauth2.NewClient(ctx, ts)

	// git client
	giteeClient := gitee.NewAPIClient(giteeConf)
	return &GiteeHandler{
		GiteeClient:giteeClient,
		Token: giteeToken,
		Context: ctx,
	}
}

func (gh *GiteeHandler) ValidateUser(wg *sync.WaitGroup, stopChannel <-chan struct{}, users <-chan string, invalid *[]string) {
	defer wg.Done()
	for {
		select {
		case u, ok := <- users:
			if !ok {
				fmt.Printf("User channel finished, quiting..\n")
				return
			} else {
				fmt.Printf("Starting to validate user %s\n", u)
				if !gh.checkUserExists(u) {
					*invalid = append(*invalid, u)
				}
			}
		case <-stopChannel:
			fmt.Println("quit signal captured, quiting.")
			return
		}
	}
}

func (gh *GiteeHandler) checkUserExists(name string) bool {
	option := gitee.GetV5UsersUsernameOpts{
		AccessToken: optional.NewString(gh.Token),
	}
	_, result, err := gh.GiteeClient.UsersApi.GetV5UsersUsername(gh.Context, name, &option)
	if err != nil {
		if result.StatusCode == 404 {
			fmt.Printf("[Warning] User %s does not exists. \n", name)
			return false
		} else {
			fmt.Printf("Failed to recognize user %s on gitee website, skipping", name)
		}
	}
	return true
}

func (gh *GiteeHandler) CollectRepoPageCount(pageSize int, enterpriseName string) int {
	option := gitee.GetV5EnterprisesEnterpriseReposOpts{
		AccessToken: optional.NewString(gh.Token),
		PerPage: optional.NewInt32(int32(pageSize)),
	}
	_, result, err := gh.GiteeClient.RepositoriesApi.GetV5EnterprisesEnterpriseRepos(gh.Context, enterpriseName, &option)
	if err != nil || result.StatusCode != 200 {

		fmt.Printf("[Error] Can't collect projects in enterprise %s, %v \n", enterpriseName, err)
		return -1
	}
	size, ok := result.Header["Total_page"]
	if !ok {
		fmt.Printf("[Error] Can't collect 'Total_page' from Header %v", result.Header)
		return -1
	}
	sizeInt, err := strconv.ParseInt(size[0], 10, 0)
	if err != nil {
		fmt.Printf("[Error] Can't convert 'Total_page' to integer %v", size)
		return -1
	}
	return int(sizeInt)
}

func (gh *GiteeHandler) CollectRepos(wg *sync.WaitGroup, pageSize, totalPage, workerIndex, gap int, enterpriseName string, rsChannel chan<- string) {
	defer wg.Done()
	for i := workerIndex; i <= totalPage; i+=gap {
		fmt.Printf("Starting to fetch project lists %d/%d from enterpise %s\n", i, totalPage, enterpriseName)
		option := gitee.GetV5EnterprisesEnterpriseReposOpts{
			AccessToken: optional.NewString(gh.Token),
			PerPage: optional.NewInt32(int32(pageSize)),
			Page: optional.NewInt32(int32(i)),
		}
		projects, result, err := gh.GiteeClient.RepositoriesApi.GetV5EnterprisesEnterpriseRepos(gh.Context, enterpriseName, &option)
		if err != nil || result.StatusCode != 200 {
			fmt.Printf("[Warning] Failed to get projects %d/%d from enterprise %s\n", i, totalPage, enterpriseName)
			continue
		}
		for _,p := range projects {
			if (!strings.HasSuffix(p.FullName, "bak")){
				rsChannel <- p.FullName
			}
		}
	}
}

func (gh *GiteeHandler) ShowRepoPRs(wg *sync.WaitGroup, owner, repo string, resultChannel chan<- PullRequest) error {
	defer wg.Done()
	options := gitee.GetV5ReposOwnerRepoPullsOpts{
		AccessToken: optional.NewString(gh.Token),
		PerPage: optional.NewInt32(int32(100)),
		State: optional.NewString("all"),
	}
	_, result, err  := gh.GiteeClient.PullRequestsApi.GetV5ReposOwnerRepoPulls(gh.Context, owner, repo, &options)
	if err != nil || result.StatusCode != 200 {

		fmt.Printf("[Error] Can't collect pull request in repo %s, %v \n", repo, err)
		return err
	}
	size, ok := result.Header["Total_page"]
	if !ok {
		fmt.Printf("[Error] Can't collect 'Total_page' from Header %v", result.Header)
		return err
	}
	sizeInt, err := strconv.ParseInt(size[0], 10, 0)
	if err != nil {
		fmt.Printf("[Error] Can't convert 'Total_page' to integer %v", size)
		return err
	}
	for i := 1; i <= int(sizeInt); i++ {
		options := gitee.GetV5ReposOwnerRepoPullsOpts{
			AccessToken: optional.NewString(gh.Token),
			PerPage: optional.NewInt32(int32(100)),
			Page: optional.NewInt32(int32(i)),
			State: optional.NewString("all"),
		}
		pulls, _, err  := gh.GiteeClient.PullRequestsApi.GetV5ReposOwnerRepoPulls(gh.Context, owner, repo, &options)
		if err != nil || len(pulls) == 0 {
			fmt.Printf("this is the pulls %v and error %v\n", pulls, err)
			continue
		}
		//fmt.Println("Star")
		for _, u := range pulls {
			//fmt.Printf("%s;", u.Name)
			PR := PullRequest{
				Id: u.Id,
				Auther:u.User.Name,
				State:u.State,
				RepoName: fmt.Sprintf("%s/%s", owner, repo),
				Number:u.Number,
				Link:u.HtmlUrl,
				CreateAt:u.CreatedAt,
			}
			resultChannel <- PR
		}
	}

	return nil
}

func (gh *GiteeHandler) ShowRepoIssues(wg *sync.WaitGroup, owner, repo string, resultChannel chan<- IssueRequest) error {
	defer wg.Done()
	optionsRepo := gitee.GetV5ReposOwnerRepoOpts{
		AccessToken: optional.NewString(gh.Token),
	}

	project, _, err  := gh.GiteeClient.RepositoriesApi.GetV5ReposOwnerRepo(gh.Context, owner, repo, &optionsRepo)

	options := gitee.GetV5ReposOwnerRepoIssuesOpts{
		//AccessToken: optional.NewString(gh.Token),
		PerPage: optional.NewInt32(int32(100)),
		State: optional.NewString("all"),
	}
	if project.Private == true {
		options = gitee.GetV5ReposOwnerRepoIssuesOpts{
			AccessToken: optional.NewString(gh.Token),
			PerPage: optional.NewInt32(int32(100)),
			State: optional.NewString("all"),
		}
	}

	_, result, err  := gh.GiteeClient.IssuesApi.GetV5ReposOwnerRepoIssues(gh.Context, owner, repo, &options)
	if err != nil || result.StatusCode != 200 {
		if result.StatusCode == 404 {
			return nil
		}
		fmt.Printf("[Error] Can't collect Issue request in repo %s, %v \n", repo, err)
		return err
	}

	size, ok := result.Header["Total_page"]
	if !ok {
		fmt.Printf("[Error] Can't collect 'Total_page' from Header %v", result.Header)
		return err
	}
	sizeInt, err := strconv.ParseInt(size[0], 10, 0)
	if err != nil {
		fmt.Printf("[Error] Can't convert 'Total_page' to integer %v", size)
		return err
	}
	for i := 1; i <= int(sizeInt); i++ {
		options := gitee.GetV5ReposOwnerRepoIssuesOpts{
			//AccessToken: optional.NewString(gh.Token),
			PerPage: optional.NewInt32(int32(100)),
			State: optional.NewString("all"),
		}
		if project.Private == true {
			options = gitee.GetV5ReposOwnerRepoIssuesOpts{
				AccessToken: optional.NewString(gh.Token),
				PerPage: optional.NewInt32(int32(100)),
				State: optional.NewString("all"),
			}
		}
		issues, _, err  := gh.GiteeClient.IssuesApi.GetV5ReposOwnerRepoIssues(gh.Context, owner, repo, &options)
		if err != nil || len(issues) == 0 {
			fmt.Printf("this is the Issues %v and error %v\n", issues, err)
			continue
		}
		//fmt.Println("Star")
		for _, u := range issues {
			//fmt.Printf("%s;", u.Name)
			PR := IssueRequest{
				Id: u.Id,
				Auther:u.User.Name,
				State:u.State,
				RepoName: fmt.Sprintf("%s/%s", owner, repo),
				Number:u.Number,
				Link:u.CommentsUrl,
			}
			resultChannel <- PR
		}
	}

	return nil
}

func (gh *GiteeHandler) ShowRepoStarStatics(wg *sync.WaitGroup, owner, repo string, resultChannel chan<- string) error {
	defer wg.Done()
	options := gitee.GetV5ReposOwnerRepoStargazersOpts{
		AccessToken: optional.NewString(gh.Token),
		PerPage: optional.NewInt32(int32(100)),
	}
	_, result, err  := gh.GiteeClient.ActivityApi.GetV5ReposOwnerRepoStargazers(gh.Context, owner, repo, &options)
	if err != nil || result.StatusCode != 200 {

		fmt.Printf("[Error] Can't collect starInfomation in repo %s, %v \n", repo, err)
		return err
	}
	size, ok := result.Header["Total_page"]
	if !ok {
		fmt.Printf("[Error] Can't collect 'Total_page' from Header %v", result.Header)
		return err
	}
	sizeInt, err := strconv.ParseInt(size[0], 10, 0)
	if err != nil {
		fmt.Printf("[Error] Can't convert 'Total_page' to integer %v", size)
		return err
	}
	for i := 1; i <= int(sizeInt); i++ {
		options := gitee.GetV5ReposOwnerRepoStargazersOpts{
			AccessToken: optional.NewString(gh.Token),
			PerPage: optional.NewInt32(int32(100)),
			Page: optional.NewInt32(int32(i)),
		}
		users, _, err  := gh.GiteeClient.ActivityApi.GetV5ReposOwnerRepoStargazers(gh.Context, owner, repo, &options)
		if err != nil || len(users) == 0 {
			fmt.Printf("this is the users %v and error %v\n", users, err)
			continue
		}
		//fmt.Println("Star")
		for _, u := range users {
			resultChannel <- u.Name
		}
	}

	return nil
}

func (gh *GiteeHandler) ShowRepoWatchStatics(wg *sync.WaitGroup, owner, repo string, resultChannel chan<- string) error {
	defer wg.Done()
	options := gitee.GetV5ReposOwnerRepoSubscribersOpts{
		AccessToken: optional.NewString(gh.Token),
		PerPage: optional.NewInt32(int32(100)),
	}
	_, result, err  := gh.GiteeClient.ActivityApi.GetV5ReposOwnerRepoSubscribers(gh.Context, owner, repo, &options)
	if err != nil || result.StatusCode != 200 {

		fmt.Printf("[Error] Can't collect subscribe Infomation in repo %s, %v \n", repo, err)
		return err
	}
	size, ok := result.Header["Total_page"]
	if !ok {
		fmt.Printf("[Error] Can't collect 'Total_page' from Header %v", result.Header)
		return err
	}
	sizeInt, err := strconv.ParseInt(size[0], 10, 0)
	if err != nil {
		fmt.Printf("[Error] Can't convert 'Total_page' to integer %v", size)
		return err
	}
	for i := 1; i <= int(sizeInt); i++ {
		options := gitee.GetV5ReposOwnerRepoSubscribersOpts{
			AccessToken: optional.NewString(gh.Token),
			PerPage: optional.NewInt32(int32(100)),
			Page: optional.NewInt32(int32(i)),
		}
		users, _, err  := gh.GiteeClient.ActivityApi.GetV5ReposOwnerRepoSubscribers(gh.Context, owner, repo, &options)
		if err != nil || len(users) == 0 {
			continue
		}
		//fmt.Println("Subscriber")
		for _, u := range users {
			//fmt.Printf("%s;", u.Name)
			resultChannel <- u.Name
		}
	}

	return nil
}


