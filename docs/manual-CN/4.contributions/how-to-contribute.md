# 如何提交代码和文档

## Step 1: 通过GitHub Fork

1. 访问 https://github.com/vesoft-inc/nebula
1. 点击右上角`Fork` 按钮创建远程分支

## Step 2: 将分支克隆到本地

定义本地工作目录：

```sh
# 定义工作目录
working_dir=$HOME/Workspace
```

将`user` 设置为GitHub账户名：

```sh
user={GitHub账户名}
```

clone代码：

```sh
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

### 定义预提交hook

请将Nebula Graph预提交挂钩链接到`.git`目录。

此挂钩检查提交格式，构建，文档生成等。

```sh
cd $working_dir/nebula/.git/hooks
ln -s ../../cpplint/bin/pre-commit.sh .
```

有时，预提交挂钩不能执行，在这种情况下，需要手动执行。

```sh
cd $working_dir/nebula/.git/hooks
chmod +x pre-commit
```

## Step 3: 分支

更新本地主分支：

```sh
cd $working_dir/nebula
git fetch upstream
git checkout master
git rebase upstream/master
```

从主分支创建并切换分支：

```sh
git checkout -b myfeature
```

**注意**
由于一个PR通常包含多个commit，在合并至master时容易被挤压(squash)，因此建议创建一个独立的分支进行更改。合并后，这个分支已无用处，因此可以使用上述rebase命令将本地master与upstream同步。此外，如果直接将commit提交至master，则需要hard reset主分支，例如：

```sh
git fetch upstream
git checkout master
git reset --hard upstream/master
git push --force origin master
```

## Step 4: 开发

### 编辑代码

此时可在 `myfeature` 分支编辑代码， 编辑时请遵循 [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)。

### 运行独立模式

如需重现并检查问题，则需在独立模式下运行nebula。

```sh
# 构建二进制文件
> make server

# 在独立模式下运行
> nebula-graphd
```

将Nebula Graph与本地服务器相连

```sh
> nebula
```

### 运行测试

```sh
# !!务必运行所有单元测试，确保所有测试顺利通过。
```

## Step 5: 保持分支同步

```sh
# 当处于myfeature分支时：
git fetch upstream
git rebase upstream/master
```

### Step 6: Commit

提交代码更改

```sh
git commit
```

### Step 7: Push

代码更改完成或需要备份代码时，将本地仓库创建的分支push到GitHub端的远程仓库：

```sh
git push -f origin myfeature
```

### Step 8: 创建pull request

1. 点击此处访问fork仓库https://github.com/$user/nebula (替换此处的 `$user` 用户名)。
1. 点击`myfeature`分支旁的 `Compare & pull request` 按钮。

### Step 9: 代码审查

公开的pull request至少需要两人审查，代码审查包括查找bug，审查代码风格等。
