# Optional git hooks

# Add one of these predefined hooks to your project hooks
A symlink can be added to your repo:
```
ln -s .git_hooks/<SOURCE_FILE> <LINK_NAME_NO_EXTENSION>
```
> Example:
> 
> `ln -s .git_hooks/pre-push.sh .git/hooks/pre-push`

Alternatively you can change your hooks' path config:
```
git config core.hooksPath .git_hooks
```

In general, the path may be absolute, or relative to the directory where the hooks are run (usually the working tree root; see DESCRIPTION section of [man githooks](https://git-scm.com/docs/githooks)).
