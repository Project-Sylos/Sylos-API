package manager

import "testing"

func TestPreferMigrationDisplayName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusName   string
		registryName string
		migrationID  string
		want         string
	}{
		{
			name:         "engine friendly name wins",
			statusName:   "Home → NAS",
			registryName: "old name",
			migrationID:  "mig-1",
			want:         "Home → NAS",
		},
		{
			name:         "registry name when status empty",
			statusName:   "",
			registryName: "My backup",
			migrationID:  "mig-1",
			want:         "My backup",
		},
		{
			name:         "registry name when status is only id",
			statusName:   "mig-1",
			registryName: "My backup",
			migrationID:  "mig-1",
			want:         "My backup",
		},
		{
			name:         "fall back to id",
			statusName:   "",
			registryName: "",
			migrationID:  "mig-1",
			want:         "mig-1",
		},
		{
			name:         "status id-like with empty registry uses id",
			statusName:   "mig-1",
			registryName: "mig-1",
			migrationID:  "mig-1",
			want:         "mig-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := preferMigrationDisplayName(tt.statusName, tt.registryName, tt.migrationID)
			if got != tt.want {
				t.Fatalf("preferMigrationDisplayName(%q,%q,%q)=%q want %q",
					tt.statusName, tt.registryName, tt.migrationID, got, tt.want)
			}
		})
	}
}
