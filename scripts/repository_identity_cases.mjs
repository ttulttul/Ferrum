#!/usr/bin/env node
// Prints how the pinned reference reads a corpus of git remote URLs, as JSON lines:
// the repository key, the source control host and the display path a project's
// repository identity takes from each remote.
//
//   node scripts/repository_identity_cases.mjs <reference-checkout> > tests/repository-identity-cases.jsonl
//
// The key and host come from the reference's built-in source control registry, as
// apps/server/src/project/RepositoryIdentityResolver.ts calls it. That file keeps its
// display-path rule (`repositoryPathOf`) private, so the four lines of it are repeated here.
import { pathToFileURL } from "node:url";
import path from "node:path";

const [reference] = process.argv.slice(2);
if (!reference) {
  console.error("usage: repository_identity_cases.mjs <reference-checkout>");
  process.exit(2);
}
// The packages export source for the bundler's `import` condition; load the files directly.
const load = (relative) => import(pathToFileURL(path.join(path.resolve(reference), "packages", relative)).href);
const { builtInSourceControlDefinitions: registry } = await load("source-control-definitions/src/registry.ts");
const { repositoryPathFromRemoteUrl } = await load("source-control-core/src/server/SourceControlProvider.ts");
const { canonicalRepositoryPath } = await load("shared/src/git.ts");

function repositoryPathOf(canonicalKey, remoteUrl) {
  const canonicalPath = canonicalKey.split("/").slice(1).join("/");
  const displayPath = repositoryPathFromRemoteUrl(remoteUrl);
  if (displayPath === null) return canonicalPath;
  return (
    [displayPath, `${displayPath}.git`].find(
      (candidate) => canonicalRepositoryPath(candidate) === canonicalRepositoryPath(canonicalPath),
    ) ?? canonicalPath
  );
}

const remotes = [
  "git@github.com:Example-Org/Example.Repo.git",
  "https://github.com/Example-Org/Example.Repo.git",
  "https://github.com/Example-Org/Example.Repo",
  "https://github.com/example/repo/",
  "ssh://git@github.com/example/repo.git",
  "git://github.com/example/repo.git",
  "https://user:token@github.com/example/repo.git",
  "git@github.example.com:team/tool.git",
  "https://octo.ghe.com/team/tool.git",
  "git@ssh.bitbucket.org:example-org/example-repo.git",
  "git@bitbucket.org:example-org/example-repo.git",
  "https://someone@bitbucket.org/example-org/example-repo.git",
  "ssh://git@bitbucket.internal.example:7999/proj/repo.git",
  "https://bitbucket.internal.example/scm/proj/repo.git",
  "git@gitlab.com:group/subgroup/project.git",
  "https://gitlab.example.com/group/project",
  "git@codeberg.org:owner/repo.git",
  "https://gitea.example.com/owner/repo.git",
  "https://forgejo.example.com:3000/owner/repo.git",
  "git@ssh.dev.azure.com:v3/org/project/repo",
  "https://org@dev.azure.com/org/project/_git/repo",
  "https://dev.azure.com/org/project/_git/repo.git",
  "org@vs-ssh.visualstudio.com:v3/org/project/repo",
  "https://org.visualstudio.com/project/_git/repo",
  "https://git.cafe/owner/repo.git",
  "https://origin.cursor.com/owner/repo.git",
  "git@example.com:owner/repo.git",
  "example.com:owner/repo.git",
  "https://example.com:8443/owner/repo.git",
  "ssh://git@[::1]:2222/owner/repo.git",
  "git@[::1]:owner/repo.git",
  "https://github.com/owner/My%20Repo.git",
  "https://github.com/owner/Ä-repo.git",
  "https://github.com/owner/repo%zz.git",
  "https://github.com/owner/repo.git?query=1#fragment",
  "git@github.com:owner/repo",
  "  git@github.com:owner/repo.git  ",
  "https://github.com/onlyowner",
  "/srv/git/repo.git",
  "file:///srv/git/repo.git",
  "C:\\repos\\thing.git",
];

for (const remoteUrl of remotes) {
  const canonicalKey = registry.repositoryKeyFromRemote(remoteUrl);
  const provider = registry.recognizeRemote(remoteUrl)?.kind ?? null;
  process.stdout.write(
    JSON.stringify({ remoteUrl, canonicalKey, provider, displayName: repositoryPathOf(canonicalKey, remoteUrl) }) + "\n",
  );
}
