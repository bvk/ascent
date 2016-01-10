// Copyright (c) 2016 BVK Chaitanya
//
// This file is part of the Ascent Library.
//
// The Ascent Library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Ascent Library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with the Ascent Library.  If not, see <http://www.gnu.org/licenses/>.

//
// A Git hook script to verify what is about to be pushed.  Called by "git
// push" after it has checked the remote status, but before anything has been
// pushed.  If this script exits with a non-zero status nothing will be pushed.
//
// Information about the commits which are being pushed is supplied as lines to
// the standard input in the form:
//
//   <local ref> <local sha1> <remote ref> <remote sha1>
//

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

func main() {
	var localRef string
	var localSha string
	var remoteRef string
	var remoteSha string

	// Scan the input parameters passed to the pre-push script.
	count, errScan := fmt.Scanf("%s %s %s %s", &localRef, &localSha, &remoteRef,
		&remoteSha)
	if errScan != nil || count != 4 {
		log.Fatalf("git input parameters are in unexpected format")
	}

	// Multiple commits can be sent in one push, so figure out the
	// uncommited commits.
	shalistCmd := exec.Command("git", "log", `--format=%H`,
		fmt.Sprintf("%s..%s", remoteSha, localSha))
	stdout, errShalist := shalistCmd.Output()
	if errShalist != nil {
		log.Fatalf("could not find uncommitted commit ids: %v", errShalist)
	}

	// Accumulate all errors and report them once.
	var errList []error
	defer func() {
		if len(errList) == 0 {
			return
		}
		fmt.Fprintln(os.Stderr)
		for _, err := range errList {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}
		fmt.Fprintln(os.Stderr)
		os.Exit(1)
	}()

	// Verify commit message format in every commit.
	for _, sha := range strings.Fields(string(stdout)) {
		commitCmd := exec.Command("git", "log", "-n", "1", sha)
		commit, errCommit := commitCmd.Output()
		if errCommit != nil {
			log.Fatalf("could not find commit message for %s: %v", sha, errCommit)
		}
		// Check the commit for compliance.
		if errs := CheckCommitMessage(sha, string(commit)); errs != nil {
			errList = append(errList, errs...)
		}
	}
}

// CheckCommitMessage verifies that commit message format includes the
// commit message template fields.
//
// See git commit template file templates/git-commit-template.txt
func CheckCommitMessage(sha, commit string) []error {
	// Regular expressions to match the fields from git commit template.
	reviewersRe := regexp.MustCompile(`^\s*Reviewers\s+:\s*\S+.*$`)
	testsRe := regexp.MustCompile(`^\s*Tests Run\s+:.*$`)
	ticketsRe := regexp.MustCompile(`^\s*Tickets Resolved\s+:.*$`)

	reviewers, tests, tickets := 0, 0, 0
	reviewersLine, testsLine, ticketsLine := -1, -1, -1
	reviewersColon, testsColon, ticketsColon := 0, 0, 0

	lines := strings.Split(commit, "\n")
	for ii, line := range lines {
		switch {
		case reviewersRe.MatchString(line):
			reviewers++
			reviewersLine = ii
			reviewersColon = strings.IndexRune(line, ':')
		case testsRe.MatchString(line):
			tests++
			testsLine = ii
			testsColon = strings.IndexRune(line, ':')
		case ticketsRe.MatchString(line):
			tickets++
			ticketsLine = ii
			ticketsColon = strings.IndexRune(line, ':')
		}
	}

	var errList []error
	if reviewers == 0 {
		err := fmt.Errorf("%s commit message doesn't have reviewers line", sha)
		errList = append(errList, err)
	}
	if reviewers > 1 {
		err := fmt.Errorf("%s commit message has multiple reviewer lines", sha)
		errList = append(errList, err)
	}

	if tests == 0 {
		err := fmt.Errorf("%s commit message doesn't have tests-run line", sha)
		errList = append(errList, err)
	}
	if tests > 1 {
		err := fmt.Errorf("%s commit message has multiple tests-run lines", sha)
		errList = append(errList, err)
	}

	if tickets == 0 {
		err := fmt.Errorf("%s commit message doesn't have tickets-resolved line",
			sha)
		errList = append(errList, err)
	}
	if tickets > 1 {
		err := fmt.Errorf("%s commit message has multiple tickets-resolved lines",
			sha)
		errList = append(errList, err)
	}

	if errList == nil {
		if reviewersColon != testsColon || testsColon != ticketsColon {
			err := fmt.Errorf("%s commit template fields are not properly aligned",
				sha)
			errList = append(errList, err)
		}

		// Commit message is found on line number 4.
		if reviewersLine == 4 {
			err := fmt.Errorf("%s a commit message must preceed the commit "+
				"template", sha)
			errList = append(errList, err)
		}

		if len(strings.TrimSpace(lines[reviewersLine-1])) != 0 {
			err := fmt.Errorf("%s must have an empty line before commit template "+
				"fields", sha)
			errList = append(errList, err)
		}

		if testsLine != reviewersLine+1 || ticketsLine != testsLine+1 {
			err := fmt.Errorf("%s commit template fields must be in order and/or "+
				"occupy only one line", sha)
			errList = append(errList, err)
		}
	}

	return errList
}
