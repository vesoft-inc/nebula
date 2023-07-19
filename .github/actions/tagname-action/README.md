# Extract tag name

Extract tag information from release branch

## Outputs

### `tag`

tag name

### `tagnum`

tag number

## Example usage

```yaml
- uses: ./.github/actions/tagname-action
  id: tag

- name: Other step
  run: |
    echo ${{ steps.tag.outputs.tag }}
    echo ${{ steps.tag.outputs.tagnum }}
```
