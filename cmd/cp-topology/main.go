// cp-topology runs the static topology Controller and its operator commands.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OraCache/oracache/internal/controller"
	"github.com/OraCache/oracache/internal/cplink"
	"github.com/OraCache/oracache/internal/scheduler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, log); err != nil {
		log.Error("cp-topology", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, log *slog.Logger) error {
	if len(args) == 0 {
		return errors.New("usage: cp-topology <plan|validate|apply|status|serve> [flags]")
	}
	command := args[0]
	switch command {
	case "plan", "dry-run", "validate", "apply", "status", "serve":
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	endpoints := flags.String("etcd-endpoints", "http://127.0.0.1:2379", "comma-separated etcd endpoints")
	prefix := flags.String("etcd-prefix", "/oracache", "etcd namespace")
	timeout := flags.Duration("timeout", 15*time.Second, "operator operation timeout")
	file := flags.String("file", "-", "input JSON file or - (stdin)")
	id := flags.String("id", "", "unique Controller process ID")
	lease := flags.Int("election-ttl", 5, "Controller election lease in seconds")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	client, err := cplink.DialEtcd(*endpoints)
	if err != nil {
		return err
	}
	defer client.Close()
	cp, err := controller.New(client, *prefix, 15*time.Second)
	if err != nil {
		return err
	}
	if command == "serve" {
		return cp.Run(ctx, *id, *lease, log)
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	switch command {
	case "status":
		status, err := cp.Status(ctx)
		if err != nil {
			return err
		}
		return encoder.Encode(status)
	case "plan", "dry-run":
		var input scheduler.Input
		if err := decode(*file, stdin, &input); err != nil {
			return err
		}
		current, err := cp.Desired(ctx)
		if err != nil {
			return err
		}
		plan, err := scheduler.Calculate(current, input)
		if err != nil {
			return err
		}
		return encoder.Encode(plan)
	case "validate", "apply":
		var plan scheduler.Plan
		if err := decode(*file, stdin, &plan); err != nil {
			return err
		}
		if command == "validate" {
			current, err := cp.Desired(ctx)
			if err != nil {
				return err
			}
			changed, err := controller.Validate(current, plan)
			if err != nil {
				return err
			}
			return encoder.Encode(controller.Result{Revision: current.Revision, Changed: changed})
		}
		result, err := cp.Apply(ctx, plan)
		if err != nil {
			return err
		}
		if err := encoder.Encode(result); err != nil {
			return err
		}
		if result.Error != "" {
			return errors.New(result.Error)
		}
		return nil
	}
	return nil
}

func decode(file string, stdin io.Reader, out any) error {
	reader := stdin
	if file != "-" {
		handle, err := os.Open(file)
		if err != nil {
			return err
		}
		defer handle.Close()
		reader = handle
	}
	decoder := json.NewDecoder(io.LimitReader(reader, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("decode operator input: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("operator input must hold one JSON document")
	}
	return nil
}
