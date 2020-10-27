# Upload file to release assets

Upload file to release assets

## Inputs

### `tag`

**Required** tag name of release branch. Default `${{ github.ref }}`.

### `file-path`

**Required** file path to be uploaded. Default `''`.

## Example usage

```yaml
uses: ./.github/actions/upload-assets-action
with:
  file-path: ${{ steps.pkg.outputs.filepath }}
  tag: ${{ steps.tag.outputs.tag  }}
```
