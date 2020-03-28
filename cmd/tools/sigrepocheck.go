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
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
	"sync"
)

type SigRepoCheck struct {
	FileName string
	GiteeToken string
	IgnoreProjects string
}



var sigRepoCheck = &SigRepoCheck{}

func SigInitRunFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sigRepoCheck.FileName, "filename", "f", "", "the file name of sig file")
	cmd.Flags().StringVarP(&sigRepoCheck.GiteeToken, "giteetoken", "g", "", "the gitee token")
	cmd.Flags().StringVarP(&sigRepoCheck.IgnoreProjects, "ignoreprojects", "i", "", "the projects should be ignored, splitted via ','")
}

func buildSigCommand() *cobra.Command {
	sigCommand := &cobra.Command{
		Use:   "sig",
		Short: "operation on sigs",
	}

	checkCommand := &cobra.Command{
		Use:   "checkrepo",
		Short: "check repo legality in sig yaml",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, CheckSigRepo())
		},
	}
	SigInitRunFlags(checkCommand)
	sigCommand.AddCommand(checkCommand)

	return sigCommand
}

func CheckSigRepo() error {
	var wg sync.WaitGroup
	var endwg sync.WaitGroup
	var totalProjects []string
	var scanProjects []string
	var invalidProjects []string
	fmt.Printf("Starting to validating all of the repos in sig file %s\n", sigRepoCheck.FileName)
	fmt.Printf("Found projects to ignore %v\n", sigRepoCheck.IgnoreProjects)
	if _, err := os.Stat(sigRepoCheck.FileName); os.IsNotExist(err) {
		return fmt.Errorf("sig file not existed %s", sigRepoCheck.FileName)
	}

	// Setting up gitee handler
	giteeHandler := NewGiteeHandler(sigRepoCheck.GiteeToken)
	sigChannel := make(chan string, 50)
	resultChannel := make(chan string, 50)

	go func() {
		endwg.Add(1)
		for rs := range resultChannel {
			totalProjects = append(totalProjects, rs)
		}
		endwg.Done()
	}()

	go func() {
		endwg.Add(1)
		for rs := range sigChannel {
			scanProjects = append(scanProjects, rs)
		}
		endwg.Done()
	}()

	// Running 5 workers to collect the projects status
	size := giteeHandler.CollectRepoPageCount(100, "open_euler")
	if size <= 0 {
		return fmt.Errorf("can't get any projects in enterprise 'open_euler'")
	}
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go giteeHandler.CollectRepos(&wg,100, size, i, 5 , "open_euler", resultChannel, )
	}
	projects := strings.Split(sigRepoCheck.IgnoreProjects, ",")
	scanner := NewDirScanner("", projects)
	err := scanner.ScanSigYaml(sigRepoCheck.FileName, sigChannel)
	//Wait all gitee query threads to be finished
	wg.Wait()
	//Close the result channels
	close(resultChannel)
	//Wait all result collection threads to be finished
	endwg.Wait()
	if err != nil {
		return err
	}
	//fmt.Printf("this is the total projects: \n %s", strings.Join(totalProjects, ";"))

	for _, scan := range scanProjects {
		if !Find(totalProjects, scan) {
			invalidProjects = append(invalidProjects, scan)
		}
	}
	if len(invalidProjects) != 0 {
		return fmt.Errorf("[Error] Failed to recognize gitee %d projects:\n %s\n", len(invalidProjects), strings.Join(invalidProjects,"\n"))
	}
	fmt.Printf("Projects successfully verified.")
	return nil
}

func Find(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

