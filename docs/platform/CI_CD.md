# CI/CD Windows Implementation Plan

## Current State

### Problem Summary
- All CI runs on `ubuntu-24.04`
- No Windows build/test jobs in any workflow
- Build setup action uses apt-get (Linux only)

### Existing CI Structure

```
.github/workflows/
├── ci.yml           (orchestrator - triggers all)
├── _build.yml       (Linux build+test)
├── _sanitizer.yml   (ASAN/TSAN on Linux)
├── _format.yml      (clang-format on Linux)
└── _docker.yml      (Docker build on Linux)

.github/actions/
└── setup-build/     (Linux setup action)
```

## Architecture Overview

```
.github/workflows/
├── ci.yml                    (orchestrator - ADD windows job)
├── _build.yml               (Linux build - existing)
├── _build-windows.yml       (NEW - Windows build)
├── _sanitizer.yml           (Linux sanitizers - existing)
├── _format.yml              (Linux format - existing)
└── _docker.yml              (Docker build - existing)

.github/actions/
├── setup-build/             (Linux setup - existing)
└── setup-build-windows/    (NEW - Windows setup)
```

## Implementation Steps

### Step 1: Create Windows Build Setup Action

**File**: `.github/actions/setup-build-windows/action.yml` (NEW)

```yaml
name: Setup Build Environment (Windows)
description: |
  Sets up the build environment for Windows builds including:
  - MSVC toolchain
  - vcpkg with Windows triplets
  - Required system libraries

runs:
  using: composite
  steps:
    - name: Checkout
      uses: actions/checkout@v4

    - name: Setup MSVC
      uses: windows-actions/setup-msvc@v3
      with:
        arch: x64
        version: 14.40 # Visual Studio 2022

    - name: Setup vcpkg
      uses: lukka/run-vcpkg@v10
      with:
        vcpkgArguments: --triplets x64-windows-static-release
        vcpkgDirectory: ${{ runner.workspace }}/vcpkg
        vcpkgRoot: ${{ runner.workspace }}/vcpkg

    - name: Verify vcpkg installed
      shell: bash
      run: |
        if [ ! -d "$VCPKG_ROOT/installed/x64-windows-static-release" ]; then
          echo "Warning: Windows triplet not installed"
          echo "Installed triplets:"
          ls -la "$VCPKG_ROOT/installed/" 2>/dev/null || echo "No triplets found"
        fi

    - name: Print build info
      shell: bash
      run: |
        echo "Visual Studio:"
        cl.exe 2>&1 | head -n 5 || echo "MSVC not in PATH"
        echo ""
        echo "vcpkg:"
        echo "  Root: $VCPKG_ROOT"
        echo "  Triplets: x64-windows-static-release"
        echo ""
        echo "CMake:"
        cmake --version 2>&1 | head -n 1 || echo "CMake not found"
```

### Step 2: Create Windows Build Workflow

**File**: `.github/workflows/_build-windows.yml` (NEW)

```yaml
name: Build (Windows)

on:
  workflow_call:
    inputs:
      config:
        type: string
        default: windows-default
      run_tests:
        type: boolean
        default: true
      artifact_suffix:
        type: string
        default: windows

jobs:
  windows-build:
    name: Build (${{ inputs.config }})
    runs-on: windows-latest
    timeout-minutes: 120

    env:
      VCPKG_ROOT: ${{ runner.workspace }}/vcpkg
      VCPKG_BINARY_SOURCE: "nuget,${{ secrets.VCPKG_NUGET_TOKEN }}@https://github.com/${{ github.repository }}/packages,readonly"

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 1

      - name: Setup build environment
        uses: ./.github/actions/setup-build-windows

      - name: Configure CMake
        shell: bash
        working-directory: ${{ github.workspace }}
        run: |
          cmake --preset ${{ inputs.config }}

      - name: Configure verbose
        if: vars.DEBUG == 'true'
        shell: bash
        run: |
          echo "Configure preset: ${{ inputs.config }}"
          echo "Binary dir: build/${{ inputs.config }}"
          cat build/${{ inputs.config }}/CMakeCache.txt | grep -E "CMAKE_SYSTEM_NAME|CMAKE_BUILD_TYPE|VCPKG_TARGET_TRIPLET" || true

      - name: Build
        shell: bash
        working-directory: ${{ github.workspace }}
        run: |
          cmake --build build/${{ inputs.config }} --parallel

      - name: Build verbose
        if: vars.DEBUG == 'true'
        shell: bash
        run: |
          cmake --build build/${{ inputs.config }} --parallel -- VERBOSE=1

      - name: List build artifacts
        shell: bash
        run: |
          echo "Build artifacts:"
          find build/${{ inputs.config }} -name "*.exe" -type f 2>/dev/null || echo "No executables found"

      - name: Upload build artifacts
        if: inputs.artifact_suffix != ''
        uses: actions/upload-artifact@v4
        with:
          name: abyss-${{ inputs.artifact_suffix }}-${{ github.run_id }}
          path: |
            build/${{ inputs.config }}/apps/abyss-server/Debug/*.exe
            build/${{ inputs.config }}/apps/abyss-server/Release/*.exe
          if-no-files-found: warn
          retention-days: 7

      - name: Run tests
        if: inputs.run_tests == 'true'
        shell: bash
        working-directory: ${{ github.workspace }}
        run: |
          ctest --preset ${{ inputs.config }} --parallel --output-on-failure

      - name: Run specific tests
        if: inputs.run_tests == 'true' && vars.DEBUG == 'true'
        shell: bash
        run: |
          ctest --preset ${{ inputs.config }} -R "poller_test|segment_test" --output-on-failure

      - name: Test Summary
        if: inputs.run_tests == 'true'
        shell: bash
        run: |
          echo "## Test Results" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "Config: ${{ inputs.config }}" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          # Note: Test results are in the test output
```

### Step 3: Update Main CI Orchestrator

**File**: `.github/workflows/ci.yml` (MODIFY)

Add Windows job to the orchestrator:

```yaml
# Original structure (simplified):
jobs:
  detect-changes:
    # ... existing ...

  linux:
    needs: detect-changes
    if: needs.detect-changes.outputs.should-run == 'true'
    uses: ./.github/workflows/_build.yml
    with:
      config: default

  windows:
    needs: detect-changes
    if: needs.detect-changes.outputs.should-run == 'true'
    uses: ./.github/workflows/_build-windows.yml
    with:
      config: windows-default
      run_tests: true
      artifact_suffix: windows

  # ... existing jobs ...
```

### Step 4: Add Sanitizer Workflow for Windows

**File**: `.github/workflows/_sanitizer-windows.yml` (NEW)

```yaml
name: Sanitizers (Windows)

on:
  workflow_call:

jobs:
  windows-asan:
    name: ASAN (Windows)
    runs-on: windows-latest
    timeout-minutes: 180

    env:
      VCPKG_ROOT: ${{ runner.workspace }}/vcpkg

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup build environment
        uses: ./.github/actions/setup-build-windows

      - name: Configure with ASAN
        shell: bash
        run: |
          cmake --preset windows-asan

      - name: Build
        shell: bash
        run: |
          cmake --build build/windows-asan --parallel

      - name: Run tests
        shell: bash
        run: |
          ctest --preset windows-asan --parallel --output-on-failure
```

### Step 5: Add Windows to Docker Build (Optional)

If you want to test Docker builds on Windows:

```yaml
# In _docker.yml, add Windows stage:
jobs:
  # ... existing Linux build ...

  windows:
    name: Build (Windows Container)
    runs-on: windows-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Build Docker image
        shell: bash
        run: |
          docker build -f Dockerfile.windows -t abyss:${{ github.sha }} .
```

Note: Windows container builds require specific Docker configuration.

### Step 6: Update NuGet Binary Cache (Optional)

The existing binary cache uses NuGet which works on Windows:

```yaml
# In setup-build-windows/action.yml, add:
- name: Setup NuGet
  uses: NuGet/setup-nuget@v2
  with:
    nuget-version: '6.x'

- name: Add NuGet source
  shell: bash
  run: |
    if [ -n "${{ secrets.VCPKG_NUGET_TOKEN }}" ]; then
      dotnet nuget add source \
        --username github \
        --password "${{ secrets.VCPKG_NUGET_TOKEN }}" \
        --store-password-in-clear-text \
        "https://nuget.pkg.github.com/${{ github.repository }}/index.json" || true
    fi
```

## Verification Checklist

- [ ] Windows CI runs successfully
- [ ] Build produces working executable
- [ ] Tests run and pass
- [ ] Artifacts are uploaded correctly
- [ ] No regressions in Linux CI

## GitHub Actions Variables

Add these repository variables for Windows CI:

| Variable | Value | Purpose |
|----------|-------|---------|
| `VCPKG_NUGET_TOKEN` | GitHub token with packages:read | Binary cache access |
| `DEBUG` | (optional) `true` | Enable verbose output |

## Matrix Testing Strategy

For comprehensive Windows testing, consider adding a matrix:

```yaml
jobs:
  windows-build:
    name: Build (${{ matrix.config }})
    runs-on: windows-latest
    strategy:
      fail-fast: false
      matrix:
        config:
          - windows-default
          - windows-release
          - windows-cloud-only
    steps:
      # ... builds with matrix config
```

## CI/CD Troubleshooting

### Common Windows CI Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| MSVC not found | Wrong architecture | Ensure x64 architecture in setup-msvc |
| vcpkg bootstrap fails | Missing dependencies | Use preinstalled vcpkg via lukka/run-vcpkg |
| Link errors | Missing libraries | Ensure triplet matches build type |
| Test timeout | Slow environment | Increase timeout-minutes |
| ASAN false positives | MSVC ASAN differences | Check MSVC ASAN documentation |

### Debugging Tips

1. **Enable verbose output**:
   - Set repository variable `DEBUG=true`
   - This enables verbose builds and test output

2. **Upload build artifacts**:
   - Artifacts include executable for manual testing
   - Download from workflow run page

3. **Check vcpkg installation**:
   - List installed triplets to verify
   - Check for dependency installation failures

## Cost Considerations

| Platform | Runner | Cost per minute |
|----------|--------|-----------------|
| ubuntu-latest | GitHub hosted | ~$0.01/min |
| windows-latest | GitHub hosted | ~$0.02/min |

Windows CI is approximately 2x the cost of Linux CI. Consider:
- Running Windows CI only on PRs that touch platform code
- Running full Windows CI nightly instead of on every push
- Using self-hosted runners for Windows if available

## Advanced: Self-Hosted Runners

If you have Windows self-hosted runners:

```yaml
jobs:
  windows-build:
    name: Build (Windows)
    runs-on: [self-hosted, windows, x64]
    # ... rest of job
```

Benefits:
- Faster startup (no VM provisioning)
- Lower cost per minute
- More control over environment

Requirements:
- Windows Server 2019+ with Visual Studio
- Pre-installed vcpkg
- Network access to GitHub Packages for binary cache
