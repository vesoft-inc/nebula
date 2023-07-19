# Upload file to oss

Upload file to oss

## Inputs

### `key-id`

**Required** access key ID of oss. Default `''`.

### `key-secret`

**Required** access key secret of oss. Default `''`.

### `endpoint`

**Required** endpoint of oss. Default `''`.

### `bucket`

**Required** bucket of oss. Default `''`.

### `asset-path`

**Required** file path to be uploaded. Default `''`.

### `target-path`

**Required** path where the oss object is stored. Default `''`.

## Example usage

```yaml
uses: ./.github/actions/upload-to-oss-action
with:
    key-id: ${{ secrets.OSS_ID }}
    key-secret: ${{ secrets.OSS_SECRET }}
    endpoint: ${{ secrets.OSS_ENDPOINT }}
    bucket: nebula-graph
    asset-path: build/cpack_output
    target-path: package/v2-nightly/${{ steps.vars.outputs.subdir }}
```
