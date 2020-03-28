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

type CheckOwnerFlags struct {
	DirName string
	FileName string
	GiteeToken string
}



var checkOwnerFlags = &CheckOwnerFlags{}

func InitRunFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&checkOwnerFlags.DirName, "dirname", "d", "", "the folder used to search the owner file")
	cmd.Flags().StringVarP(&checkOwnerFlags.FileName, "filename", "f", "", "the file name of owner file")
	cmd.Flags().StringVarP(&checkOwnerFlags.GiteeToken, "giteetoken", "g", "", "the gitee token")
}

func buildOwnerCommand() *cobra.Command {
	ownerCommand := &cobra.Command{
		Use:   "owner",
		Short: "operation on owners",
	}

	checkCommand := &cobra.Command{
		Use:   "check",
		Short: "check owner legacy on gitee website",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, CheckOwner())
		},
	}
	InitRunFlags(checkCommand)
	ownerCommand.AddCommand(checkCommand)

	return ownerCommand
}

func CheckOwner() error {
	var wg sync.WaitGroup
	var failedUser []string
	fmt.Printf("Starting to validating all of the owner files in dir %s\n", checkOwnerFlags.DirName)
	if _, err := os.Stat(checkOwnerFlags.DirName); os.IsNotExist(err) {
		return fmt.Errorf("directory not existed %s", checkOwnerFlags.DirName)
	}

	// Setting up gitee owner handler
	giteeHandler := NewGiteeHandler(checkOwnerFlags.GiteeToken)
	userChannel := make(chan string, 20)
	stopCh := SetupSignalHandler()
	wg.Add(1)
	go giteeHandler.ValidateUser(&wg, stopCh, userChannel, &failedUser)

	var emptyProjects []string
	scanner := NewDirScanner(checkOwnerFlags.DirName, emptyProjects)
	err := scanner.ScanAllOwners(checkOwnerFlags.FileName, userChannel)
	wg.Wait()
	if err != nil {
		return err
	}
	if len(failedUser) != 0 {
		return fmt.Errorf("[Error] Failed to recognize gitee user:\n %s\n", strings.Join(failedUser,"\n"))
	}
	fmt.Printf("Owners successfully verified.")
	return nil
}

