## Releasing

This document contains a general description and tips for releasing Influxdb3-java using Github Actions with the ultimate goal of pushing the release to Maven Central.

### Overview

Releasing involves three general steps.

1. Preparing the release in a new release branch
2. Triggering the automatic release by creating a new tag and release in Github.
3. Preparing the next release cycle by merging the next release branch created by the automated release script back into `main`.

### Preparing the release.

1. from `main` create a new release branch, e.g. `git checkout -b chore/release-1.12.0`
2. In the new branch, update the version settings in `pom.xml`

```
$ mvn versions:set -DremoveSnapshot=true
$ mvn versions:set-scm-tag  -DnewTag="v1.12.0"
```
3. In `README.md` update the `<version>` tag value in the Maven dependency example.
4. In `CHANGELOG.md` verify all commit information for the current cycle is up-to-date, fix any discrepancies, then update the date for this release to the current date.
5. In `examples/pom.xml` update the version of the Influxdb3-java dependency to the version to be released.
6. Commit and push these changes to Github.

### Initiating the release

In Github `influxComunit/influxb3-java`...

1. In the project home page open the _Releases_ section.
2. Click _Draft a new release_
3. In the _Target_ dropdown check the new release branch.
4. In the _Tag_ dropdown click `Create new tag`, supply the tag matching the value added in the `set-scm-tag` command above.  Note that the tag should be prefixed with the letter _v_.  Click _Create_.
5. In the _Release title_ text control set the title to match the `<version>` tag in `pom.xml`.
6. In the _Release notes_ text field copy changes added since the last release from `CHANEGLOG.md`.
7. If this is a pre-release check the _pre-release_ radio button.  Note that for "pre-release" releases the release workflow will not publish builds to Maven Central, will not publish site documentation and will not prepare the next release cycle.  This setting can be useful for debugging the workflow.
8. Click `Publish release`.

The creation of the new release will trigger the `maven-release.yml` workflow.  It will:

1. Verify required secrets and environment variables are set
2. Check that the `pom.xml` version matches the release tag and that versioning references in documentation and examples are up-to-date and valid.
3. Check and sign the build.
4. Upload the archives and pom files to Maven Central.
5. Publish site documentation to https://github.com/influxcommunity/influxdb3-java/tree/gh-pages
6. prepare the `pom.xml` and `CHANGELOG.md` files for the next release cycle and push them to a new branch named `ci/next-cycle-<NEXT_RELEASE_VERSION>`.

### Publish release in Maven Central.

From the Maven Central account, review and publish the release to make it available to the public.

### Preparing the next release cycle

A new branch `ci/next-cycle-<NEXT_RELEASE_VERSION>` with an updated `pom.xml` file will have been created by the Maven Release action.

1. create a PR from the branch `ci/next-cycle-<NEXT_RELEASE_VERSION>` to merge it into `main`.
2. review the PR and if everything has been generated correctly, squash and merge it.

## The Release environment

The release workflow is managed by `.github/workflows/maven-release.yml`.  In order for the release workflow to succeed a number of environment secrets need to be set in the project. It may be necessary to update these in the future.

- `GPG_PASSPHRASE` - password for the key used to sign archives and pom files to be uploaded to Maven Central.  See _Generating a new GPG signing key_ below.
- `GPG_PRIVATE_KEY` - private key associate with public key pulled from a GPG repository and used to sign archives and pom files.
- `SONATYPE_PASSWORD` - password for the user account used to upload archives and pom files to Maven Central.
- `SONATYPE_USERNAME` - name of user account used to upload archives and pom files to Maven Central.

### Addenda

#### Generating a new GPG signing key

1. Generate a random password for the key.

e.g. On a Linux box...

```
$ head -c 6 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9+-'
jEtkLlyG
```
Store this somewhere safe.

2. Generate a key with a passphrase and no expiration.

```
$ gpg --batch --passphrase=<GENERATED_PASSPHRASE> --quick-generate-key "karel-rehor@users.noreply.github.com" default default never
gpg: revocation certificate stored as '/home/karl/.gnupg/openpgp-revocs.d/REDACTED_KEY_ID.rev'
```

3. Verify key.

```
$ gpg --list-keys
gpg: checking the trustdb
gpg: marginals needed: 3  completes needed: 1  trust model: pgp
gpg: depth: 0  valid:   1  signed:   0  trust: 0-, 0q, 0n, 0m, 0f, 1u
/home/karl/.gnupg/pubring.kbx
-----------------------------
pub   ed25519 2026-09-03 [SC]
      REDACTED_KEY_ID
uid           [ultimate] karel-rehor@users.noreply.github.com
sub   cv25519 2026-09-03 [E]
```

4. Distribute the key.

```
$ gpg2 --keyserver keyserver.ubuntu.com --send-keys REDACTED_KEY_ID
gpg: sending key REDACETD to hkp://keyserver.ubuntu.com
```

5. Verify key is on remote.  Note, that it may take a few minutes to be registered.

```
$ gpg2 --keyserver keyserver.ubuntu.com --search-keys REDACTED_KEY_ID
gpg: data source: http://185.125.188.27:11371
(1)	karel-rehor@users.noreply.github.com
	  263 bit EDDSA key REDACTED, created: 2026-09-03
Keys 1-1 of 1 for "REDACTED_KEY_ID".  Enter number(s), N)ext, or Q)uit > 1
gpg: key REDACTED: "karel-rehor@users.noreply.github.com" not changed
gpg: Total number processed: 1
gpg:              unchanged: 1
```

6. Get the secret key associated with this key.  It will need to be copied then pasted to the Github project secret GPG_PRIVATE_KEY.  Note you will be prompted for the passphrase.

```
$ gpg2 --export-secret-keys --armor REDACTED_KEY_ID
-----BEGIN PGP PRIVATE KEY BLOCK-----

   ...REDACTED...

-----END PGP PRIVATE KEY BLOCK-----
```
#### Revoking a compromised GPG2 key


1. verify that the key is on the local server.

```
$ gpg2 --list-keys
/home/karl/.gnupg/pubring.kbx
-----------------------------
pub   ed25519 2026-07-27 [SC] [expires: 2029-07-26]
      KEY_ID_REDACTED
uid           [ultimate] karel-rehor@users.noreply.github.com
sub   REDACTED 2026-07-27 [E]

```

or...

```
$ gpg2 --list-keys KEY_ID_REDACTED
pub   ed25519 2026-07-27 [SC] [expires: 2029-07-26]
      KEY_ID_REDACTED
uid           [ultimate] karel-rehor@users.noreply.github.com
sub   REDACTED 2026-07-27 [E]
```

2. Verify that the key is on the remote server. 

```
$ gpg2 --keyserver keyserver.ubuntu.com --search-keys KEY_ID_REDACTED
gpg: data source: http://185.125.188.27:11371
(1)	karel-rehor@users.noreply.github.com
	  263 bit EDDSA key REDACTED, created: 2026-07-27
Keys 1-1 of 1 for "KEY_ID_REDACTED".  Enter number(s), N)ext, or Q)uit > 1
gpg: key REDACTED: "karel-rehor@users.noreply.github.com" not changed
gpg: Total number processed: 1
gpg:              unchanged: 1
```

3. Create a revocation request locally. 

```
$ gpg2 --output revoke-karel-rehor.asc --gen-revoke KEY_ID_REDACTED
...
```

4. Revoke the key locally.

```
$ gpg2 --import revoke-karel-rehor.asc 
gpg: key REDACTED: "karel-rehor@users.noreply.github.com" revocation certificate imported
gpg: Total number processed: 1
gpg:    new key revocations: 1
gpg: marginals needed: 3  completes needed: 1  trust model: pgp
gpg: depth: 0  valid:   1  signed:   0  trust: 0-, 0q, 0n, 0m, 0f, 1u
gpg: next trustdb check due at 2029-07-26
```

5. Verify revocation succeeded. 

```
$ gpg2 --list-keys
/home/karl/.gnupg/pubring.kbx
-----------------------------
pub   ed25519 2026-07-27 [SC] [revoked: 2026-09-03]
      KEY_ID_REDACTED
uid           [ revoked] karel-rehor@users.noreply.github.com
```

6. Push change of key state to remote server. 

```
$ gpg2 --keyserver keyserver.ubuntu.com --send-keys KEY_ID_REDACTED
gpg: sending key REDACTED to hkp://keyserver.ubuntu.com
```

7. Verify key state on remote server.

```
$ gpg2 --keyserver keyserver.ubuntu.com --search-keys KEY_ID_REDACTED
gpg: data source: http://185.125.188.27:11371
gpg: key "KEY_ID_REDACTED" not found on keyserver
```

8. Delete secret key locally. 

```
$ gpg2 --delete-secret-key KEY_ID_REDACTED
...
# Confirmation required
```

9. Delete key locally

```
$ gpg2 --delete-key 01EAECEC736391172C6520F48B5778484117B952
...
# Confirmation required
```

10. Verify key is deleted

```
$ gpg2 --list-keys
gpg: checking the trustdb
gpg: no ultimately trusted keys found
```
