package gui

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"time"

	"github.com/fatih/color"
	"github.com/jesseduffield/lazydocker/pkg/commands"
	"github.com/jesseduffield/lazydocker/pkg/tasks"
	"github.com/jesseduffield/lazydocker/pkg/utils"
)

func (gui *Gui) renderContainerLogsToMain(container *commands.Container) tasks.TaskFunc {
	return gui.NewTickerTask(TickerTaskOpts{
		Func: func(ctx context.Context, notifyStopped chan struct{}) {
			gui.renderContainerLogsToMainAux(container, ctx, notifyStopped)
		},
		Duration: time.Millisecond * 200,
		// TODO: see why this isn't working (when switching from Top tab to Logs tab in the services panel, the tops tab's content isn't removed)
		Before:     func(ctx context.Context) { gui.clearMainView() },
		Wrap:       gui.Config.UserConfig.Gui.WrapMainPanel,
		Autoscroll: true,
	})
}

func (gui *Gui) renderContainerLogsToMainAux(container *commands.Container, ctx context.Context, notifyStopped chan struct{}) {
	gui.clearMainView()
	defer func() {
		notifyStopped <- struct{}{}
	}()

	mainView := gui.Views.Main

	if err := gui.writeContainerLogs(container, ctx, mainView); err != nil {
		gui.Log.Error(err)
	}

	// if we are here because the task has been stopped, we should return
	// if we are here then the container must have exited, meaning we should wait until it's back again before
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := container.Inspect()
			if err != nil {
				// if we get an error, then the container has probably been removed so we'll get out of here
				gui.Log.Error(err)
				return
			}
			if result.State.Running {
				return
			}
		}
	}
}

func (gui *Gui) renderLogsToStdout(container *commands.Container) {
	stop := make(chan os.Signal, 1)
	defer signal.Stop(stop)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		signal.Notify(stop, os.Interrupt)
		<-stop
		cancel()
	}()

	if err := gui.g.Suspend(); err != nil {
		gui.Log.Error(err)
		return
	}

	defer func() {
		if err := gui.g.Resume(); err != nil {
			gui.Log.Error(err)
		}
	}()

	if err := gui.writeContainerLogs(container, ctx, os.Stdout); err != nil {
		gui.Log.Error(err)
		return
	}

	gui.promptToReturn()
}

func (gui *Gui) promptToReturn() {
	if !gui.Config.UserConfig.Gui.ReturnImmediately {
		fmt.Fprintf(os.Stdout, "\n\n%s", utils.ColoredString(gui.Tr.PressEnterToReturn, color.FgGreen))

		// wait for enter press
		if _, err := fmt.Scanln(); err != nil {
			gui.Log.Error(err)
		}
	}
}

func (gui *Gui) writeContainerLogs(ctr *commands.Container, ctx context.Context, writer io.Writer) error {
	// Use CLI command instead of Docker API for compatibility with Podman
	var cmd *exec.Cmd
	if gui.Config.UsePodman {
		cmd = gui.OSCommand.ExecutableFromString(fmt.Sprintf("podman logs --follow %s", ctr.ID))
	} else {
		cmd = gui.OSCommand.ExecutableFromString(fmt.Sprintf("docker logs --follow %s", ctr.ID))
	}
	
	// Add log options
	if gui.Config.UserConfig.Logs.Timestamps {
		if gui.Config.UsePodman {
			cmd = gui.OSCommand.ExecutableFromString(fmt.Sprintf("podman logs --follow --timestamps %s", ctr.ID))
		} else {
			cmd = gui.OSCommand.ExecutableFromString(fmt.Sprintf("docker logs --follow --timestamps %s", ctr.ID))
		}
	}
	
	if gui.Config.UserConfig.Logs.Since != "" {
		if gui.Config.UsePodman {
			cmd = gui.OSCommand.ExecutableFromString(fmt.Sprintf("podman logs --follow --since=%s %s", gui.Config.UserConfig.Logs.Since, ctr.ID))
		} else {
			cmd = gui.OSCommand.ExecutableFromString(fmt.Sprintf("docker logs --follow --since=%s %s", gui.Config.UserConfig.Logs.Since, ctr.ID))
		}
	}
	
	if gui.Config.UserConfig.Logs.Tail != "" {
		if gui.Config.UsePodman {
			cmd = gui.OSCommand.ExecutableFromString(fmt.Sprintf("podman logs --follow --tail=%s %s", gui.Config.UserConfig.Logs.Tail, ctr.ID))
		} else {
			cmd = gui.OSCommand.ExecutableFromString(fmt.Sprintf("docker logs --follow --tail=%s %s", gui.Config.UserConfig.Logs.Tail, ctr.ID))
		}
	}
	
	// Set up the command with proper output handling
	cmd.Stdout = writer
	cmd.Stderr = writer
	
	// Start the command
	if err := cmd.Start(); err != nil {
		gui.Log.Error(err)
		return err
	}
	
	// Wait for context cancellation or command completion
	go func() {
		<-ctx.Done()
		cmd.Process.Kill()
	}()
	
	return cmd.Wait()
}
