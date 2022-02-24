# Contributing to doric

Thank you for investing your time in contributing to our project! 🎊

In this guide you will get an overview of the contribution workflow
from opening an issue, creating a PR, reviewing, and merging the PR.

## New contributor guide

To get an overview of the project, read the [README](README.md).

## Getting started

### Issues

#### Create a new issue

If you spot a problem, [search if an issue already exists](https://github.com/hablapps/doric/issues).
If a related issue doesn't exist, you can open a new issue using a relevant [issue form](https://github.com/hablapps/doric/issues/new/choose).

#### Solve an issue

Scan through our [existing issues](https://github.com/github/docs/issues)
to find one that interests you. As a general rule, we don’t assign issues to anyone.
If you find an issue to work on, you are welcome to open a PR with a fix.

### Code contribution

Please, follow the official [Scala style guide](http://docs.scala-lang.org/style/).
We also use `scalafmt` to check and format scala code, so you could use
any of the following commands to help you:

```bash
scalafmtCheckAll # Simply checks the code and reports style errors
scalafmtAll      # Checks the code and reformats everything that needs formatting
```

#### Fork

If you want to contribute solving issues or adding some features, the first thing
you should do is to fork the repository.

Your fork repository will copy the current [doric GitHub Actions](https://github.com/hablapps/doric/actions),
so you may want to keep it synced and use some of them.

#### Commits

Whenever possible, please follow the [Conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) specification

#### Pull Request

- Even if you haven't finished your contribution, 
we encourage you to push your code and create a PR as a
[draft](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests#draft-pull-requests)
so everybody knows it is a working in progress
- Don't forget to [link PR to issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue).

### Your PR is merged!

Congratulations!! 🎉🎉 The doric team thanks you ✨.

We use the squash method while merging, so you will only see just one commit for your contribution.
