## Workflow
We welcome contributions to Nebula of any kind including documentation, organizaiton, tutorials, blog posts, bug reports, issues, feature requests, feature implementations, pull requests, helping to manage issues, etc. We created a step by step guide if you're unfamiliar with GitHub or contributing to open source projects in general.

### Setting Up the Development Environment
#### Prerequisites
* Install [Git](https://git-scm.com/) (may be already installed on your system, or available through your OS package manager)
* The project Nebula is developed using C++14, so it requires a compiler supporting C++14 features. Here we recommend GCC(starting from version 5.0) and Clang(starting from 3.4 )

### Step 1: Fetching the Sources From GitHub

1. Visit https://github.com/vesoft-inc/nebula
2. Click `Fork` button (top right) to establish a cloud-based fork

### Step 2: Clone fork to local storage

Define a local working directory:

```sh
# Define your working directory
working_dir=$HOME/Workspace
```

Set `user` to match your Github profile name:

```sh
user={your Github profile name}
```

Create your clone:

```sh
mkdir -p $working_dir
cd $working_dir
git clone https://github.com/$user/nebula.git
# the above is recommended
# or: git clone git@github.com:$user/nebula.git

cd $working_dir/nebula
git remote add upstream https://github.com/vesoft-inc/nebula.git
# or: git remote add upstream git@github.com:vesoft-inc/nebula.git

# Never push to upstream master since you do not have write access.
git remote set-url --push upstream no_push

# Confirm that your remotes make sense:
# It should look like the follow:
# origin    git@github.com:$(user)/nebula.git (fetch)
# origin    git@github.com:$(user)/nebula.git (push)
# upstream  https://github.com/vesoft-inc/nebula (fetch)
# upstream  no_push (push)
git remote -v
```

#### Define a pre-commit hook

Please link the Nebula Graph pre-commit hook into your `.git` directory.

This hook checks your commits for formatting, building, doc generation, etc.

```sh
cd $working_dir/nebula/.git/hooks
ln -s ../../cpplint/bin/pre-commit.sh .
```
Sometimes, pre-commit hook might not be executable. In such case, you have to make it executable manually.

```sh
cd $working_dir/nebula/.git/hooks
chmod +x pre-commit
```

### Step 3: Branch

Get your local master up to date:

```sh
cd $working_dir/nebula
git fetch upstream
git checkout master
git rebase upstream/master
```

Branch from master:

```sh
git checkout -b myfeature
```
**NOTE**: Since your PR often consists of several commits, which might be squashed while being merged into upstream,
we strongly suggest you open a separate topic branch to make your changes. After merged, this topic branch could be just abandoned, thus you could synchronize your master branch with the upstream easily with the rebase above. Otherwise, if you commit your changes directly into master, you must use a hard reset on the master branch as follow:

```sh
git fetch upstream
git checkout master
git reset --hard upstream/master
git push --force origin master
```

### Step 4: Develop

#### Edit the code

You can now edit the code on `myfeature` branch. Please follow the coding style guidance [here](docs/cpp-coding-style.md).

#### Run stand-alone mode

If you want to reproduce and investigate an issue, you may need
to run Nebula Graph in stand-alone mode.

```sh
# Build the binary.
make server

# Run in stand-alone mode.
nebula-graphd
```

Then you can connect the Nebula Graph console to your local server
```sh
nebula
```

#### Run Test

```sh
# Run unit test to make sure all tests passed.
```

### Step 5: Keep your branch in sync

```sh
# While on your myfeature branch:
git fetch upstream
git rebase upstream/master
```

### Step 6: Commit

Commit your changes.

```sh
git commit
```

You can go back and repeat the edit/build/test process with `commit --amend` if necessary.

### Step 7: Push

When ready to review (or just to establish an offsite backup for your work), push your branch to your fork on `github.com`:

```sh
git push -f origin myfeature
```

### Step 8: Create a pull request

1. Visit your fork at https://github.com/user/nebula (replace `user` with your github profile name).
2. Click the `Compare & pull request` button next to your `myfeature` branch.

### Step 9: Get a code review

Once your pull request has been submitted, it will be assigned to at least one reviewers who will conduct a thorough code review, looking for correctness, bugs, opportunities for improvement, documentation, comments, style, etc.

Small PRs are easy to review while the large ones are not, so please be patient before our response.
