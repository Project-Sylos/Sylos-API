package migrations

import (
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Spectra/sdk"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fslib "codeberg.org/Sylos/Sylos-FS/pkg/fs"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// acquireSharedSpectraAdapters checks if both services use the same Spectra config and shares a session if they do
// Returns (srcAdapter, dstAdapter, sharedSession, error)
// If not shareable (different configs or not both Spectra), returns (nil, nil, false, nil)
func (m *Manager) acquireSharedSpectraAdapters(srcCfg, dstCfg migration.ServiceConfigYAML, spectraConfigOverridePath string) (fstypes.FSAdapter, fstypes.FSAdapter, bool, error) {
	if strings.ToLower(srcCfg.Type) != "spectra" || strings.ToLower(dstCfg.Type) != "spectra" {
		return nil, nil, false, nil
	}

	getConfigPath := func(serviceCfg migration.ServiceConfigYAML) (string, error) {
		if spectraConfigOverridePath != "" {
			return spectraConfigOverridePath, nil
		}

		def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
		if err == nil && def.Spectra != nil {
			return def.Spectra.ConfigPath, nil
		}

		world := "primary"
		if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
			world = "s1"
		}
		def, err = m.serviceMgr.GetServiceDefinitionByWorld(world)
		if err == nil && def.Spectra != nil {
			return def.Spectra.ConfigPath, nil
		}

		return "", fmt.Errorf("spectra config path not found for service %s", serviceCfg.Name)
	}

	srcConfigPath, err := getConfigPath(srcCfg)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to get source config path: %w", err)
	}

	dstConfigPath, err := getConfigPath(dstCfg)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to get destination config path: %w", err)
	}

	if srcConfigPath != dstConfigPath {
		return nil, nil, false, nil
	}

	m.logger.Info().
		Str("config_path", srcConfigPath).
		Msg("source and destination use same Spectra config, sharing SDK session")

	spectraFS, err := sdk.New(srcConfigPath)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to create shared SpectraFS session: %w", err)
	}

	srcRootID := srcCfg.RootID
	if srcRootID == "" {
		srcRootID = "root"
	}

	srcWorld := "primary"
	if strings.Contains(strings.ToLower(srcCfg.Name), "s1") {
		srcWorld = "s1"
	} else {
		def, err := m.serviceMgr.GetServiceDefinition(srcCfg.Name)
		if err == nil && def.Spectra != nil {
			srcWorld = def.Spectra.World
		}
	}

	isEphemeral, err := services.IsEphemeralMode(srcConfigPath)
	if err != nil {
		m.logger.Warn().Err(err).Str("config_path", srcConfigPath).Msg("failed to detect ephemeral mode, defaulting to persistent")
		isEphemeral = false
	}

	srcAdapter, err := fslib.NewSpectraFS(spectraFS, srcRootID, srcWorld, isEphemeral)
	if err != nil {
		_ = spectraFS.Close()
		return nil, nil, false, fmt.Errorf("failed to create source adapter: %w", err)
	}

	dstRootID := dstCfg.RootID
	if dstRootID == "" {
		dstRootID = "root"
	}

	dstWorld := "primary"
	if strings.Contains(strings.ToLower(dstCfg.Name), "s1") {
		dstWorld = "s1"
	} else {
		def, err := m.serviceMgr.GetServiceDefinition(dstCfg.Name)
		if err == nil && def.Spectra != nil {
			dstWorld = def.Spectra.World
		}
	}

	dstAdapter, err := fslib.NewSpectraFS(spectraFS, dstRootID, dstWorld, isEphemeral)
	if err != nil {
		_ = spectraFS.Close()
		return nil, nil, false, fmt.Errorf("failed to create destination adapter: %w", err)
	}

	m.logger.Info().
		Str("src_root", srcRootID).
		Str("src_world", srcWorld).
		Str("dst_root", dstRootID).
		Str("dst_world", dstWorld).
		Str("config_path", srcConfigPath).
		Msg("created shared Spectra adapters")

	m.logger.Debug().
		Str("src_name", srcCfg.Name).
		Str("src_type", srcCfg.Type).
		Str("src_root_id", srcCfg.RootID).
		Str("dst_name", dstCfg.Name).
		Str("dst_type", dstCfg.Type).
		Str("dst_root_id", dstCfg.RootID).
		Msg("adapter configuration details")

	return srcAdapter, dstAdapter, true, nil
}

// acquireAdapterFromYAMLConfig acquires an adapter based on YAML service configuration
func (m *Manager) acquireAdapterFromYAMLConfig(serviceCfg migration.ServiceConfigYAML, spectraConfigOverridePath string) (fstypes.FSAdapter, error) {
	serviceType := strings.ToLower(serviceCfg.Type)
	switch serviceType {
	case "spectra":
		configPath := spectraConfigOverridePath
		if configPath == "" {
			def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
			if err == nil && def.Spectra != nil {
				configPath = def.Spectra.ConfigPath
			} else {
				world := "primary"
				if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
					world = "s1"
				}
				def, err := m.serviceMgr.GetServiceDefinitionByWorld(world)
				if err == nil && def.Spectra != nil {
					configPath = def.Spectra.ConfigPath
				} else {
					return nil, fmt.Errorf("spectra config path not found for service %s", serviceCfg.Name)
				}
			}
		}

		spectraFS, err := sdk.New(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SpectraFS: %w", err)
		}

		rootID := serviceCfg.RootID
		if rootID == "" {
			rootID = "root"
		}

		world := "primary"
		if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
			world = "s1"
		} else {
			def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
			if err == nil && def.Spectra != nil {
				world = def.Spectra.World
			} else {
				def, err := m.serviceMgr.GetServiceDefinitionByWorld(world)
				if err == nil && def.Spectra != nil {
					world = def.Spectra.World
				}
			}
		}

		isEphemeral, err := services.IsEphemeralMode(configPath)
		if err != nil {
			m.logger.Warn().Err(err).Str("config_path", configPath).Msg("failed to detect ephemeral mode, defaulting to persistent")
			isEphemeral = false
		}

		adapter, err := fslib.NewSpectraFS(spectraFS, rootID, world, isEphemeral)
		if err != nil {
			_ = spectraFS.Close()
			return nil, fmt.Errorf("failed to create SpectraFS adapter: %w", err)
		}

		return adapter, nil

	case "local":
		rootPath := serviceCfg.RootPath
		if rootPath == "" {
			return nil, fmt.Errorf("local service %s missing root path", serviceCfg.Name)
		}

		adapter, err := fslib.NewLocalFS(rootPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create LocalFS adapter: %w", err)
		}

		return adapter, nil

	default:
		return nil, fmt.Errorf("unsupported service type: %s", serviceType)
	}
}
