## Workflow

### Step 1: Fork in the cloud

1. Visit https://github.com/vesoft-inc/vgraph
2. Click `Fork` button (top right) to establish a cloud-based fork.

### Step 2: Clone fork to local storage

Define a local working directory:

```sh
# Define your working directory
working_dir=$HOME/Workspace
```

Set `user` to match your github profile name:

```sh
user={your github profile name}
```

Create your clone:

```sh
mkdir -p $working_dir
cd $working_dir
git clone https://github.com/$user/vgraph.git
# the following is recommended
# or: git clone git@github.com:$user/vgraph.git

cd $working_dir/vgraph
git remote add upstream https://github.com/vesoft-inc/vgraph.git
# or: git remote add upstream git@github.com:vesoft-inc/vgraph.git

# Never push to upstream master since you do not have write access.
git remote set-url --push upstream no_push

# Confirm that your remotes make sense:
# It should look like:
# origin    git@github.com:$(user)/vgraph.git (fetch)
# origin    git@github.com:$(user)/vgraph.git (push)
# upstream  https://github.com/vesoft-inc/vgraph (fetch)
# upstream  no_push (push)
git remote -v
```

#### Define a pre-commit hook

Please link the vGraph pre-commit hook into your `.git` directory.

This hook checks your commits for formatting, building, doc generation, etc.

```sh
cd $working_dir/vgraph/.git/hooks
ln -s ../../hooks/pre-commit .
```
Sometime, pre-commit hook can not be executable. In such case, you have to make it executable manually.

```sh
cd $working_dir/vgraph/.git/hooks
chmod +x pre-commit
```

### Step 3: Branch

Get your local master up to date:

```sh
cd $working_dir/vgraph
git fetch upstream
git checkout master
git rebase upstream/master
```

Branch from master:

```sh
git checkout -b myfeature
```
**NOTE**: Because your PR often consists of several commits, which might be squashed while being merged into upstream,
we strongly suggest you open a separate topic branch to make your changes on. After merged,
this topic branch could be just abandoned, thus you could synchronize your master branch with
upstream easily with a rebase like above. Otherwise, if you commit your changes directly into master,
maybe you must use a hard reset on the master branch, like:

```sh
git fetch upstream
git checkout master
git reset --hard upstream/master
git push --force origin master
```

### Step 4: Develop

#### Edit the code

You can now edit the code on the `myfeature` branch. Please follow the coding style guidance [here](docs/cpp-coding-style.md)

#### Run stand-alone mode

If you want to reproduce and investigate an issue, you may need
to run vGraph in stand-alone mode.

```sh
# Build the binary.
make server

# Run in stand-alone mode.
vgraphd
```

Then you can connect the vGraph console to your local server
```sh
vgraph
```

#### Run Test

```sh
# Run unit test to make sure all test passed.
```

### Step 5: Keep your branch in sync

```sh
# While on your myfeature branch.
git fetch upstream
git rebase upstream/master
```

### Step 6: Commit

Commit your changes.

```sh
git commit
```

Likely you'll go back and edit/build/test some more than `commit --amend`
in a few cycles.

### Step 7: Push

When ready to review (or just to establish an offsite backup or your work),
push your branch to your fork on `github.com`:

```sh
git push -f origin myfeature
```

### Step 8: Create a pull request

1. Visit your fork at https://github.com/$user/vgraph (replace `$user` obviously).
2. Click the `Compare & pull request` button next to your `myfeature` branch.

### Step 9: Get a code review

Once your pull request has been opened, it will be assigned to at least one
reviewers. Those reviewers will do a thorough code review, looking for
correctness, bugs, opportunities for improvement, documentation and comments,
and style.

Commit changes made in response to review comments to the same branch on your
fork.

Very small PRs are easy to review. Very large PRs are very difficult to
review.
