# 如何提交代码和文档

## Step 1: 通过 GitHub Fork

1. 访问 https://github.com/vesoft-inc/nebula
1. 点击右上角 `Fork` 按钮创建远程分支

## Step 2: 将分支克隆到本地

定义本地工作目录：

```bash
# 定义工作目录
working_dir=$HOME/Workspace
```

将 `user` 设置为 GitHub 账户名：

```bash
user={GitHub账户名}
```

clone 代码：

```bash
mkdir -p $working_dir
cd $working_dir
git clone https://github.com/$user/nebula.git
# 推荐如下方式
# 或: git clone git@github.com:$user/nebula.git

cd $working_dir/nebula
git remote add upstream https://github.com/vesoft-inc/nebula.git
# 或: git remote add upstream git@github.com:vesoft-inc/nebula.git

# 由于没有写访问权限，请勿推送至上游主分支
git remote set-url --push upstream no_push

# 确认远程分支有效：
# 正确的格式为：
# origin    git@github.com:$(user)/nebula.git (fetch)
# origin    git@github.com:$(user)/nebula.git (push)
# upstream  https://github.com/vesoft-inc/nebula (fetch)
# upstream  no_push (push)
git remote -v
```

### 定义预提交 hook

请将 **Nebula Graph** 预提交挂钩链接到 `.git` 目录。

此挂钩检查提交格式，构建，文档生成等。

```bash
cd $working_dir/nebula/.git/hooks
ln -s ../../cpplint/bin/pre-commit.sh .
```

有时，预提交挂钩不能执行，在这种情况下，需要手动执行。

```bash
cd $working_dir/nebula/.git/hooks
chmod +x pre-commit
```

## Step 3: 分支

更新本地主分支：

```bash
cd $working_dir/nebula
git fetch upstream
git checkout master
git rebase upstream/master
```

从主分支创建并切换分支：

```bash
git checkout -b myfeature
```

**注意**
由于一个 PR 通常包含多个 commit，在合并至 master 时容易被挤压 ( squash )，因此建议创建一个独立的分支进行更改。合并后，这个分支已无用处，因此可以使用上述 rebase 命令将本地 master 与 upstream 同步。此外，如果直接将 commit 提交至 master，则需要 hard reset 主分支，例如：

```bash
git fetch upstream
git checkout master
git reset --hard upstream/master
git push --force origin master
```

## Step 4: 开发

### 编辑代码

此时可在 `myfeature` 分支编辑代码， 编辑时请遵循 [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)。

### 对修改的代码进行验证
#### 编译源码

请参考文档[build-source-code](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-CN/3.build-develop-and-administration/1.build/1.build-source-code.md)

#### 验证
- 替换二进制文件

编译好的三个服务的二进制文件在 `nebula/build/src/daemon/_build/` 目录下面，编译好的 console 在 `nebula/build/src/console/_build` 目录下面。可以把二进制替换到安装目录 `bin` 下面，重启服务并做验证。

- 添加单元测试

在修改的模块代码目录下面会有个test目录，可以在里面添加单元测试，然后编译运行单元测试，提交的代码必须确保所有单元测试顺利通过。

运行所有单元测试

```bash
cd nebula/build
ctest -j$(nproc)
```

## Step 5: 保持分支同步

```bash
# 当处于 myfeature 分支时：
git fetch upstream
git rebase upstream/master
```

### Step 6: Commit

提交代码更改

```bash
git commit
```

### Step 7: Push

代码更改完成或需要备份代码时，将本地仓库创建的分支 push 到 GitHub 端的远程仓库：

```bash
git push -f origin myfeature
```

### Step 8: 创建 pull request

1. 点击此处访问 fork 仓库https://github.com/$user/nebula (替换此处的 `$user` 用户名)。
1. 点击 `myfeature` 分支旁的 `Compare & pull request` 按钮。

### Step 9: 代码审查

公开的 pull request 至少需要两人审查，代码审查包括查找 bug，审查代码风格等。
