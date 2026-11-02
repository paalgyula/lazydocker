package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/bindings/images"
	podmanNetwork "github.com/containers/podman/v5/pkg/bindings/network"
	"github.com/containers/podman/v5/pkg/bindings/volumes"
	dockerTypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/volume"
	"github.com/imdario/mergo"
	"github.com/jesseduffield/lazydocker/pkg/config"
	"github.com/jesseduffield/lazydocker/pkg/i18n"
	"github.com/jesseduffield/lazydocker/pkg/utils"
	"github.com/sasha-s/go-deadlock"
	"github.com/sirupsen/logrus"
)

// PodmanCommand is our main podman interface
type PodmanCommand struct {
	Log                    *logrus.Entry
	OSCommand              *OSCommand
	Tr                     *i18n.TranslationSet
	Config                 *config.AppConfig
	InDockerComposeProject bool
	ErrorChan              chan error
	ContainerMutex         deadlock.Mutex
	ServiceMutex           deadlock.Mutex
	Connection             context.Context

	Closers []io.Closer
}

var _ io.Closer = &PodmanCommand{}

// NewPodmanCommand creates a new Podman command interface
func NewPodmanCommand(log *logrus.Entry, osCommand *OSCommand, tr *i18n.TranslationSet, config *config.AppConfig, errorChan chan error) (*PodmanCommand, error) {
	// Create Podman connection using ONLY Podman API
	var connection context.Context
	var err error

	// Try different Podman socket locations, starting with user's rootless socket
	uid := os.Getuid()
	userSocket := fmt.Sprintf("unix:///run/user/%d/podman/podman.sock", uid)

	socketPaths := []string{
		userSocket,                           // User's rootless socket (default)
		"unix:///run/podman/podman.sock",     // System-wide rootful socket
		"unix:///var/run/podman/podman.sock", // Alternative system socket
	}

	// Check CONTAINER_HOST environment variable first
	if containerHost := os.Getenv("CONTAINER_HOST"); containerHost != "" {
		socketPaths = append([]string{containerHost}, socketPaths...)
	}

	var lastErr error
	for _, socketPath := range socketPaths {
		log.Debugf("Trying Podman socket: %s", socketPath)
		connection, err = bindings.NewConnection(context.Background(), socketPath)
		if err == nil {
			log.Debugf("Successfully connected to Podman at: %s", socketPath)
			break
		}
		lastErr = err
		log.Debugf("Failed to connect to %s: %v", socketPath, err)
	}

	if connection == nil || lastErr != nil {
		return nil, fmt.Errorf("failed to connect to Podman (tried %v): %w", socketPaths, lastErr)
	}

	podmanCommand := &PodmanCommand{
		Log:                    log,
		OSCommand:              osCommand,
		Tr:                     tr,
		Config:                 config,
		ErrorChan:              errorChan,
		InDockerComposeProject: true,
		Connection:             connection,
		Closers:                []io.Closer{},
	}

	podmanCommand.setDockerComposeCommand(config)

	err = osCommand.RunCommand(
		utils.ApplyTemplate(
			config.UserConfig.CommandTemplates.CheckDockerComposeConfig,
			podmanCommand.NewCommandObject(CommandObject{}),
		),
	)
	if err != nil {
		podmanCommand.InDockerComposeProject = false
		log.Warn(err.Error())
	}

	return podmanCommand, nil
}

func (c *PodmanCommand) setDockerComposeCommand(config *config.AppConfig) {
	if config.UserConfig.CommandTemplates.DockerCompose != "docker compose" {
		return
	}

	// Check if 'docker compose' is available, fall back to 'docker-compose' if not
	err := c.OSCommand.RunCommand("docker compose version")
	if err != nil {
		config.UserConfig.CommandTemplates.DockerCompose = "docker-compose"
	}
}

func (c *PodmanCommand) Close() error {
	return utils.CloseMany(c.Closers)
}

func (c *PodmanCommand) NewCommandObject(obj CommandObject) CommandObject {
	defaultObj := CommandObject{DockerCompose: c.Config.UserConfig.CommandTemplates.DockerCompose}
	_ = mergo.Merge(&defaultObj, obj)
	return defaultObj
}

func (c *PodmanCommand) CreateClientStatMonitor(container *Container) {
	container.MonitoringStats = true

	go func() {
		defer func() {
			container.MonitoringStats = false
		}()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			// Get container stats using Podman API
			statsChan, err := containers.Stats(c.Connection, []string{container.ID}, &containers.StatsOptions{})
			if err != nil {
				c.Log.Error(err)
				continue
			}

			statReport := <-statsChan
			if statReport.Error != nil {
				c.Log.Error(statReport.Error)
				continue
			}

			if len(statReport.Stats) == 0 {
				continue
			}

			stat := statReport.Stats[0]

			// Create a simple ContainerStats with minimal data
			clientStats := ContainerStats{
				CPUStats: struct {
					CPUUsage struct {
						TotalUsage        int64   `json:"total_usage"`
						PercpuUsage       []int64 `json:"percpu_usage"`
						UsageInKernelmode int64   `json:"usage_in_kernelmode"`
						UsageInUsermode   int64   `json:"usage_in_usermode"`
					} `json:"cpu_usage"`
					SystemCPUUsage int64 `json:"system_cpu_usage"`
					OnlineCpus     int   `json:"online_cpus"`
					ThrottlingData struct {
						Periods          int `json:"periods"`
						ThrottledPeriods int `json:"throttled_periods"`
						ThrottledTime    int `json:"throttled_time"`
					} `json:"throttling_data"`
				}{
					CPUUsage: struct {
						TotalUsage        int64   `json:"total_usage"`
						PercpuUsage       []int64 `json:"percpu_usage"`
						UsageInKernelmode int64   `json:"usage_in_kernelmode"`
						UsageInUsermode   int64   `json:"usage_in_usermode"`
					}{
						TotalUsage: int64(stat.CPU * 1000000000), // Convert to nanoseconds
					},
				},
				MemoryStats: struct {
					Usage    int `json:"usage"`
					MaxUsage int `json:"max_usage"`
					Stats    struct {
						ActiveAnon              int   `json:"active_anon"`
						ActiveFile              int   `json:"active_file"`
						Cache                   int   `json:"cache"`
						Dirty                   int   `json:"dirty"`
						HierarchicalMemoryLimit int64 `json:"hierarchical_memory_limit"`
						HierarchicalMemswLimit  int64 `json:"hierarchical_memsw_limit"`
						InactiveAnon            int   `json:"inactive_anon"`
						InactiveFile            int   `json:"inactive_file"`
						MappedFile              int   `json:"mapped_file"`
						Pgfault                 int   `json:"pgfault"`
						Pgmajfault              int   `json:"pgmajfault"`
						Pgpgin                  int   `json:"pgpgin"`
						Pgpgout                 int   `json:"pgpgout"`
						Rss                     int   `json:"rss"`
						RssHuge                 int   `json:"rss_huge"`
						TotalActiveAnon         int   `json:"total_active_anon"`
						TotalActiveFile         int   `json:"total_active_file"`
						TotalCache              int   `json:"total_cache"`
						TotalDirty              int   `json:"total_dirty"`
						TotalInactiveAnon       int   `json:"total_inactive_anon"`
						TotalInactiveFile       int   `json:"total_inactive_file"`
						TotalMappedFile         int   `json:"total_mapped_file"`
						TotalPgfault            int   `json:"total_pgfault"`
						TotalPgmajfault         int   `json:"total_pgmajfault"`
						TotalPgpgin             int   `json:"total_pgpgin"`
						TotalPgpgout            int   `json:"total_pgpgout"`
						TotalRss                int   `json:"total_rss"`
						TotalRssHuge            int   `json:"total_rss_huge"`
						TotalUnevictable        int   `json:"total_unevictable"`
						TotalWriteback          int   `json:"total_writeback"`
						Unevictable             int   `json:"unevictable"`
						Writeback               int   `json:"writeback"`
					} `json:"stats"`
					Limit int64 `json:"limit"`
				}{
					Usage: int(stat.MemUsage),
					Limit: int64(stat.MemLimit),
				},
				PrecpuStats: struct {
					CPUUsage struct {
						TotalUsage        int64   `json:"total_usage"`
						PercpuUsage       []int64 `json:"percpu_usage"`
						UsageInKernelmode int64   `json:"usage_in_kernelmode"`
						UsageInUsermode   int64   `json:"usage_in_usermode"`
					} `json:"cpu_usage"`
					SystemCPUUsage int64 `json:"system_cpu_usage"`
					OnlineCpus     int   `json:"online_cpus"`
					ThrottlingData struct {
						Periods          int `json:"periods"`
						ThrottledPeriods int `json:"throttled_periods"`
						ThrottledTime    int `json:"throttled_time"`
					} `json:"throttling_data"`
				}{
					CPUUsage: struct {
						TotalUsage        int64   `json:"total_usage"`
						PercpuUsage       []int64 `json:"percpu_usage"`
						UsageInKernelmode int64   `json:"usage_in_kernelmode"`
						UsageInUsermode   int64   `json:"usage_in_usermode"`
					}{
						TotalUsage: int64(stat.CPU * 1000000000),
					},
				},
			}

			recordedStats := &RecordedStats{
				ClientStats: clientStats,
				DerivedStats: DerivedStats{
					CPUPercentage:    stat.CPU,
					MemoryPercentage: stat.MemPerc,
				},
				RecordedAt: time.Now(),
			}

			container.appendStats(recordedStats, c.Config.UserConfig.Stats.MaxDuration)
		}
	}()
}

func (c *PodmanCommand) RefreshContainersAndServices(currentServices []*Service, currentContainers []*Container) ([]*Container, []*Service, error) {
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()

	containers, err := c.GetContainers(currentContainers)
	if err != nil {
		return nil, nil, err
	}

	var services []*Service
	if currentServices != nil {
		services = currentServices
	} else {
		services, err = c.GetServices()
		if err != nil {
			return nil, nil, err
		}
	}

	c.assignContainersToServices(containers, services)

	return containers, services, nil
}

func (c *PodmanCommand) assignContainersToServices(containers []*Container, services []*Service) {
L:
	for _, service := range services {
		for _, ctr := range containers {
			if !ctr.OneOff && ctr.ServiceName == service.Name {
				service.Container = ctr
				continue L
			}
		}
		service.Container = nil
	}
}

// GetContainers gets the podman containers using ONLY Podman API
func (c *PodmanCommand) GetContainers(existingContainers []*Container) ([]*Container, error) {
	c.ContainerMutex.Lock()
	defer c.ContainerMutex.Unlock()

	// Get containers from Podman using ONLY Podman API
	containerList, err := containers.List(c.Connection, &containers.ListOptions{All: &[]bool{true}[0]})
	if err != nil {
		return nil, err
	}

	ownContainers := make([]*Container, len(containerList))

	for i, ctr := range containerList {
		var newContainer *Container

		// check if we already have data stored against the container
		for _, existingContainer := range existingContainers {
			if existingContainer.ID == ctr.ID {
				newContainer = existingContainer
				break
			}
		}

		// initialise the container if it's completely new
		if newContainer == nil {
			newContainer = &Container{
				ID:            ctr.ID,
				OSCommand:     c.OSCommand,
				Log:           c.Log,
				DockerCommand: c,
				Tr:            c.Tr,
			}
		}

		// Convert Podman container to Docker container format
		ports := make([]dockerTypes.Port, len(ctr.Ports))
		for j, port := range ctr.Ports {
			ports[j] = dockerTypes.Port{
				IP:          port.HostIP,
				PrivatePort: uint16(port.ContainerPort),
				PublicPort:  uint16(port.HostPort),
				Type:        port.Protocol,
			}
		}

		// Convert Podman container to Docker container format
		command := ""
		if len(ctr.Command) > 0 {
			command = ctr.Command[0]
		}
		newContainer.Container = dockerTypes.Container{
			ID:      ctr.ID,
			Names:   ctr.Names,
			Image:   ctr.Image,
			ImageID: ctr.ImageID,
			Command: command,
			Created: ctr.Created.Unix(),
			State:   ctr.State,
			Status:  ctr.Status,
			Ports:   ports,
			Labels:  ctr.Labels,
		}

		// Set container name
		if name, ok := ctr.Labels["name"]; ok {
			newContainer.Name = name
		} else if len(ctr.Names) > 0 {
			newContainer.Name = strings.TrimLeft(ctr.Names[0], "/")
		}
		newContainer.ServiceName = ctr.Labels["com.docker.compose.service"]
		newContainer.ProjectName = ctr.Labels["com.docker.compose.project"]
		newContainer.ContainerNumber = ctr.Labels["com.docker.compose.container"]
		newContainer.OneOff = ctr.Labels["com.docker.compose.oneoff"] == "True"

		ownContainers[i] = newContainer
	}

	// Set container details using ONLY Podman API
	c.SetContainerDetails(ownContainers)

	return ownContainers, nil
}

// GetServices gets services
func (c *PodmanCommand) GetServices() ([]*Service, error) {
	if !c.InDockerComposeProject {
		return nil, nil
	}

	composeCommand := c.Config.UserConfig.CommandTemplates.DockerCompose
	output, err := c.OSCommand.RunCommandWithOutput(fmt.Sprintf("%s config --services", composeCommand))
	if err != nil {
		return nil, err
	}

	lines := utils.SplitLines(output)
	services := make([]*Service, len(lines))
	for i, str := range lines {
		services[i] = &Service{
			Name:          str,
			ID:            str,
			OSCommand:     c.OSCommand,
			Log:           c.Log,
			DockerCommand: c,
		}
	}

	return services, nil
}

func (c *PodmanCommand) RefreshContainerDetails(containers []*Container) error {
	// Use the same method as DockerCommand
	c.SetContainerDetails(containers)
	return nil
}

// SetContainerDetails attaches the details returned from podman inspect to each of the containers using ONLY Podman API
func (c *PodmanCommand) SetContainerDetails(containerList []*Container) {
	wg := sync.WaitGroup{}
	for _, ctr := range containerList {
		wg.Add(1)
		go func(cont *Container) {
			defer wg.Done()
			details, err := containers.Inspect(c.Connection, cont.ID, &containers.InspectOptions{})
			if err != nil {
				c.Log.Error(err)
				return
			}
			// Create a simplified Docker inspect format - just set basic fields
			cont.Details = dockerTypes.ContainerJSON{
				ContainerJSONBase: &container.ContainerJSONBase{
					ID:      details.ID,
					Created: details.Created.Format(time.RFC3339),
					Path:    details.Path,
					Args:    details.Args,
					State: &container.State{
						Status:     details.State.Status,
						Running:    details.State.Running,
						Paused:     details.State.Paused,
						Restarting: details.State.Restarting,
						OOMKilled:  details.State.OOMKilled,
						Dead:       details.State.Dead,
						Pid:        details.State.Pid,
						ExitCode:   int(details.State.ExitCode),
						Error:      details.State.Error,
						StartedAt:  details.State.StartedAt.Format(time.RFC3339),
						FinishedAt: details.State.FinishedAt.Format(time.RFC3339),
					},
					Image:           details.Image,
					ResolvConfPath:  details.ResolvConfPath,
					HostnamePath:    details.HostnamePath,
					HostsPath:       details.HostsPath,
					Name:            details.Name,
					RestartCount:    int(details.RestartCount),
					Driver:          details.Driver,
					MountLabel:      details.MountLabel,
					ProcessLabel:    details.ProcessLabel,
					AppArmorProfile: details.AppArmorProfile,
					ExecIDs:         details.ExecIDs,
					HostConfig:      &container.HostConfig{},
				},
			}
		}(ctr)
	}
	wg.Wait()
}

// ViewAllLogs attaches to a subprocess viewing all the logs from docker-compose
func (c *PodmanCommand) ViewAllLogs() (*exec.Cmd, error) {
	cmd := c.OSCommand.ExecutableFromString(
		utils.ApplyTemplate(
			c.OSCommand.Config.UserConfig.CommandTemplates.ViewAllLogs,
			c.NewCommandObject(CommandObject{}),
		),
	)

	c.OSCommand.PrepareForChildren(cmd)

	return cmd, nil
}

// DockerComposeConfig returns the result of 'docker-compose config'
func (c *PodmanCommand) DockerComposeConfig() string {
	output, err := c.OSCommand.RunCommandWithOutput(
		utils.ApplyTemplate(
			c.OSCommand.Config.UserConfig.CommandTemplates.DockerComposeConfig,
			c.NewCommandObject(CommandObject{}),
		),
	)
	if err != nil {
		output = err.Error()
	}
	return output
}

// RefreshVolumes gets the podman volumes using ONLY Podman API
func (c *PodmanCommand) RefreshVolumes() ([]*Volume, error) {
	// Get volumes from Podman using ONLY Podman API
	volumeList, err := volumes.List(c.Connection, &volumes.ListOptions{})
	if err != nil {
		return nil, err
	}

	ownVolumes := make([]*Volume, len(volumeList))
	for i, vol := range volumeList {
		// Convert Podman volume to Docker volume format
		dockerVolume := volume.Volume{
			Name:       vol.Name,
			Driver:     vol.Driver,
			Mountpoint: vol.Mountpoint,
			CreatedAt:  vol.CreatedAt.Format(time.RFC3339),
			Labels:     vol.Labels,
			Scope:      vol.Scope,
			Options:    vol.Options,
		}

		ownVolumes[i] = &Volume{
			Name:          vol.Name,
			Volume:        &dockerVolume,
			OSCommand:     c.OSCommand,
			Log:           c.Log,
			DockerCommand: c,
		}
	}

	return ownVolumes, nil
}

// PruneVolumes removes unused volumes
func (c *PodmanCommand) PruneVolumes() error {
	_, err := volumes.Prune(c.Connection, &volumes.PruneOptions{})
	return err
}

// PruneContainers removes stopped containers
func (c *PodmanCommand) PruneContainers() error {
	_, err := containers.Prune(c.Connection, &containers.PruneOptions{})
	return err
}

// RefreshImages gets the podman images using ONLY Podman API
func (c *PodmanCommand) RefreshImages() ([]*Image, error) {
	// Get images from Podman using ONLY Podman API
	imageList, err := images.List(c.Connection, &images.ListOptions{})
	if err != nil {
		return nil, err
	}

	ownImages := make([]*Image, len(imageList))
	for i, img := range imageList {
		// Extract name and tag from RepoTags
		name := ""
		tag := ""
		if len(img.RepoTags) > 0 {
			parts := strings.Split(img.RepoTags[0], ":")
			if len(parts) >= 2 {
				name = parts[0]
				tag = parts[1]
			} else {
				name = img.RepoTags[0]
				tag = "latest"
			}
		}

		// Convert Podman image to Docker image format
		dockerImage := image.Summary{
			ID:          img.ID,
			ParentID:    img.ParentId,
			RepoTags:    img.RepoTags,
			RepoDigests: img.RepoDigests,
			Created:     img.Created,
			Size:        img.Size,
			VirtualSize: img.VirtualSize,
			Labels:      img.Labels,
		}

		ownImages[i] = &Image{
			Name:          name,
			Tag:           tag,
			ID:            img.ID,
			Image:         dockerImage,
			OSCommand:     c.OSCommand,
			Log:           c.Log,
			DockerCommand: c,
		}
	}

	return ownImages, nil
}

// PruneImages removes unused images
func (c *PodmanCommand) PruneImages() error {
	_, err := images.Prune(c.Connection, &images.PruneOptions{})
	return err
}

// RefreshNetworks gets the podman networks using ONLY Podman API
func (c *PodmanCommand) RefreshNetworks() ([]*Network, error) {
	// Get networks from Podman using ONLY Podman API
	networkList, err := podmanNetwork.List(c.Connection, &podmanNetwork.ListOptions{})
	if err != nil {
		return nil, err
	}

	ownNetworks := make([]*Network, len(networkList))
	for i, net := range networkList {
		// Convert Podman network to Docker network format
		dockerNetwork := network.Inspect{
			ID:       net.ID,
			Name:     net.Name,
			Driver:   net.Driver,
			Internal: net.Internal,
			Labels:   net.Labels,
		}

		ownNetworks[i] = &Network{
			Name:          net.Name,
			Network:       dockerNetwork,
			OSCommand:     c.OSCommand,
			Log:           c.Log,
			DockerCommand: c,
		}
	}

	return ownNetworks, nil
}

// PruneNetworks removes unused networks using ONLY Podman API
func (c *PodmanCommand) PruneNetworks() error {
	_, err := podmanNetwork.Prune(c.Connection, &podmanNetwork.PruneOptions{})
	return err
}
