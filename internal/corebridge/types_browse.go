package corebridge

import fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

type ServiceType = fstypes.ServiceType

const (
	ServiceTypeLocal   = fstypes.ServiceTypeLocal
	ServiceTypeSpectra = fstypes.ServiceTypeSpectra
	ServiceTypeCloud   = fstypes.ServiceTypeCloud
)

const (
	RootRoleSource      = "source"
	RootRoleDestination = "destination"
)

type Source struct {
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	Type        ServiceType       `json:"type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ListChildrenRequest struct {
	ServiceID    string
	Identifier   string
	Role         string // "source" or "destination" - used to map "spectra" to the correct world
	ConnectionID string // Cloud/Spectra session connection ID
	RootType     string // Cloud browse root type when listing a virtual root
	DriveID      string // Cloud namespace metadata (Dropbox team_folder, shared_folder)
	Offset       int    // Pagination offset (default: 0)
	Limit        int    // Pagination limit (default: 100, max: 1000)
	FoldersOnly  bool   // If true, only return folders and apply limit to folders only
}

type BrowseNodeRef struct {
	ID   string `json:"id"`
	Type string `json:"type"` // "file" | "folder"
}

type BrowseMutationRequest struct {
	ServiceID    string
	ConnectionID string
	Role         string
	RootType     string
	DriveID      string
	ContextID    string
}

type CreateBrowseFolderRequest struct {
	ParentID     string `json:"parentId"`
	Name         string `json:"name"`
	ConnectionID string `json:"connectionId,omitempty"`
	Role         string `json:"role,omitempty"`
	RootType     string `json:"rootType,omitempty"`
	DriveID      string `json:"driveId,omitempty"`
}

type DeleteBrowseNodesRequest struct {
	Nodes        []BrowseNodeRef `json:"nodes"`
	ConnectionID string          `json:"connectionId,omitempty"`
	Role         string          `json:"role,omitempty"`
	RootType     string          `json:"rootType,omitempty"`
	DriveID      string          `json:"driveId,omitempty"`
	ContextID    string          `json:"contextId,omitempty"`
}

type DeleteBrowseNodeError struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

type DeleteBrowseNodesResponse struct {
	Deleted []string                `json:"deleted"`
	Errors  []DeleteBrowseNodeError `json:"errors"`
}

// ListChildrenResponse wraps the list result with pagination metadata
type ListChildrenResponse struct {
	Folders    []fstypes.Folder `json:"folders"`
	Files      []fstypes.File   `json:"files"`
	Pagination PaginationInfo   `json:"pagination"`
}

// PaginationInfo provides pagination metadata
type PaginationInfo struct {
	Offset       int    `json:"offset"`               // Current offset
	Limit        int    `json:"limit"`                // Current limit
	Total        *int   `json:"total,omitempty"`      // Total items when known; omitted/nil when unknown (e.g. search hot path)
	TotalFolders int    `json:"totalFolders"`         // Total number of folders
	TotalFiles   int    `json:"totalFiles"`           // Total number of files
	HasMore      bool   `json:"hasMore"`              // Whether there are more items beyond the current page
	NextCursor   string `json:"nextCursor,omitempty"` // Keyset cursor for next page (path of last item; use as afterPath)
}

// DriveInfo is the Sylos-FS drive/volume descriptor returned by ListDrives.
type DriveInfo = fstypes.DriveInfo

// StorageInfo is best-effort capacity / free space for a service root or account.
type StorageInfo = fstypes.StorageInfo

// GetStorageInfoRequest asks for capacity for a service (path for local; connection for cloud).
type GetStorageInfoRequest struct {
	ServiceID    string
	Path         string
	ConnectionID string
	RootType     string
	DriveID      string
	Role         string
}

type MountDriveRequest struct {
	Device string `json:"device"`
}

type FolderDescriptor struct {
	ID           string `json:"id"`
	ParentID     string `json:"parentId,omitempty"`
	ParentPath   string `json:"parentPath,omitempty"`
	DisplayName  string `json:"displayName,omitempty"`
	LocationPath string `json:"locationPath,omitempty"`
	LastUpdated  string `json:"lastUpdated,omitempty"`
	DepthLevel   int    `json:"depthLevel,omitempty"`
	Type         string `json:"type,omitempty"`
}
