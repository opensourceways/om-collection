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
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"fmt"
	"path/filepath"
)

type SigsYaml struct {
	Sigs []Sig `yaml:"sigs"`
}

type Sig struct {
	Name         string   `yaml:"name"`
	Repositories []string `yaml:"repositories"`
}

type DirScanner struct {
	DirName string
	ignoreProjects []string
}

func NewDirScanner(dir string, projects []string) *DirScanner {
	return &DirScanner{
		DirName: dir,
		ignoreProjects: projects,
	}
}

func (ds *DirScanner) ScanAllOwners(filename string, users chan<- string) error {
	defer close(users)
	//Scan all owner files
	var files []string
	err := filepath.Walk(ds.DirName, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && info.Name() == "sig-template" {
			return filepath.SkipDir
		}
		if info.Name() == filename {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to scan owner file from folder %s \n", checkOwnerFlags.DirName)
	}
	for _, f := range files {
		owners, err := ioutil.ReadFile(f)
		if err != nil {
			fmt.Printf("Unable to open owner file %s, skipping\n", f)
			continue
		}
		ow := make(map[string] []string)
		err = yaml.Unmarshal(owners, &ow)
		if err != nil {
			fmt.Printf("Unable to read owner yaml file %s with error %v, skipping\n", f, err)
			continue
		}

		if _, ok := ow["maintainers"]; !ok {
			fmt.Printf("Owner file does't have 'maintainers' configured: %v, skipping\n", ow)
			continue
		}
		for _, w := range ow["maintainers"] {
			users <- w
		}
	}
	return nil
}

func (ds *DirScanner) ScanSigYaml(filename string, projects chan<- string) error {
	defer close(projects)
	sigFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("Unable to open sig file %s, skipping\n", filename)
	}
	sig := SigsYaml{}
	err = yaml.Unmarshal(sigFile, &sig)
	if err != nil {
		return fmt.Errorf("Unable to read sig yaml file %s with error %v, skipping\n", filename, err)
	}

	for _,s := range sig.Sigs {
		for _, repo := range s.Repositories {
			if Find(ds.ignoreProjects, repo) {
				fmt.Printf("[Warning] Project %s will be ignored due to --ignoreproject options %v\n", repo, ds.ignoreProjects)
			} else {
				projects <- repo
			}
		}
	}
	return nil
}

