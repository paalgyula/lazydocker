package commands

import (
	"io"
	"os/exec"
)

// ContainerCommand defines the interface that both DockerCommand and PodmanCommand must implement
type ContainerCommand interface {
	io.Closer
	NewCommandObject(CommandObject) CommandObject
	CreateClientStatMonitor(container *Container)
	RefreshContainersAndServices(currentServices []*Service, currentContainers []*Container) ([]*Container, []*Service, error)
	GetContainers(existingContainers []*Container) ([]*Container, error)
	GetServices() ([]*Service, error)
	RefreshContainerDetails(containers []*Container) error
	ViewAllLogs() (*exec.Cmd, error)
	DockerComposeConfig() string
	RefreshVolumes() ([]*Volume, error)
	PruneVolumes() error
	PruneContainers() error
	RefreshImages() ([]*Image, error)
	PruneImages() error
	RefreshNetworks() ([]*Network, error)
	PruneNetworks() error
}