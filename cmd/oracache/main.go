package main

import (
	"github.com/OraCache/oracache/cmd/oracache/cli"
	"os"
)

func main() { os.Exit(cli.Run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }
