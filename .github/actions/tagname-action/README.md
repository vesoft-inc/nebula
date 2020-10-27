# Extract tag name

Extract tag name from release branch

## Outputs

### `tag`

tag name

## Example usage

```yaml
- uses: ./.github/actions/tagname-action
  id: tag

- name: Other step
  run: echo ${{ steps.tag.outputs.tag }}
```
