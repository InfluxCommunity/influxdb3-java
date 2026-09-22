#!/usr/bin/env bash
#
# The MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

set -e

echo "Starting ${0}"

if [ -z "${GITHUB_ACTIONS}" ]
then
  echo "This script can only be run in a GitHub action container."
  echo "Local runs are not yet supported."
  exit 1
fi
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR="${SCRIPT_DIR}/.."
CHANGELOG_PATH="${PROJECT_DIR}/CHANGELOG.md"
README_PATH="${PROJECT_DIR}/README.md"
POM_XML_PATH="${PROJECT_DIR}/pom.xml"
EXAMPLE_POM_XML_PATH="${PROJECT_DIR}/examples/pom.xml"

RELEASE_NUM=""
RC_OR_BETA=false

FAILURE_BOILERPLATE="Please delete the tag ${RELEASE_TAG_NAME} and the related release, and start again."

github_check(){

  if [ "${GITHUB_EVENT_NAME}" == "workflow_dispatch" ]; then
    echo "WARNING: This script is targeted for 'release' not ${GITHUB_EVENT_NAME}."
    echo "Continuing for debugging."
    return
  fi

  if [ "${GITHUB_EVENT_NAME}" == "push" ]; then
    RELEASE_TAG_MATCH="^v[0-9]+(\.[0-9]+){2}(-(rc|beta)[0-9]+)?$"
    echo "WARNING: this script is targeted for 'release' not ${GITHUB_EVENT_NAME}"

    if [[ "$GITHUB_REF_NAME" =~ $RELEASE_TAG_MATCH ]]; then
      echo "Detected matching tag value in GITHUB_REF_NAME (${GITHUB_REF_NAME}).  Continuing for debugging purposes."
      RELEASE_TAG_NAME="${GITHUB_REF_NAME}"
      return
    else
      printf 'GITHUB_REF_NAME=%s failed to match valid release tag pattern.\n' "${GITHUB_REF_NAME}"
      exit 1
    fi
  fi

  if [ "${GITHUB_EVENT_NAME}" != "release" ]; then
    echo "This script can run only on 'release'.  Detected GitHub event ${GITHUB_EVENT_NAME}."
    exit 1
  fi

  if ! echo "${RELEASE_TAG_NAME}" | grep -Ei '^v[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)[0-9]*)?$'
  then
    echo "This script requires a valid release tag (e.g. v1.9.0), but RELEASE_TAG_NAME=${RELEASE_TAG_NAME} was found."
    exit 1
  fi

  echo "Running ${GITHUB_EVENT_NAME} with tag ${RELEASE_TAG_NAME}."
}

setup(){
  ADDITIONAL_INSTALLS=""
  if ! [ -x "$(command -v xmllint)" ]
  then
    ADDITIONAL_INSTALLS="${ADDITIONAL_INSTALLS} libxml2-utils"
  else
    printf "have xmllint\n"
  fi

  if [ -n "${ADDITIONAL_INSTALLS}" ]
  then
    printf "This script requires the following unavailable libraries: %s\n" "${ADDITIONAL_INSTALLS}"
    printf "Please install them and then continue.\n"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  else
    printf "Additional requirements already satisfied.\n"
  fi
}

set_release_number(){
    RELEASE_NUM=$(echo "${RELEASE_TAG_NAME}" | sed -r "s/^v//" | sed -r "s/-(rc|beta)[0-9]*//i" )
}

verify_rc_or_beta(){
  LOWER_TAG_NAME=$(echo "${RELEASE_TAG_NAME}" | tr '[:upper:]' '[:lower:]')
  if echo "${LOWER_TAG_NAME}" | grep -q "rc\|beta"
  then
    RC_OR_BETA=true
  fi
}

verify_changelog() {

  CHANGELOG_RELEASE_HEADERS=$(sed -n '/^#.*[0-9].[0-9]*.[0-9].*/p' "${CHANGELOG_PATH}")

  mapfile -t HEADER_ARRAY <<< "$CHANGELOG_RELEASE_HEADERS"

  mapfile -td ' ' HEADER_LINE <<< "${HEADER_ARRAY[0]}"

  HEADER_TAG="${HEADER_LINE[1]}"
  HEADER_DATE="${HEADER_LINE[2]:1:-2}"

  if [ "$HEADER_TAG" != "$RELEASE_NUM"  ]; then
    printf "ERROR: Latest HEADER_TAG in CHANGELOG.md (%s) does not match release number (%s) from git tag (%s)\n" \
    "$HEADER_TAG" \
    "$RELEASE_NUM" \
    "$RELEASE_TAG_NAME"
    printf "Please update the latest HEADER_TAG in CHANGELOG.md\n"
    printf "%s" "${FAILURE_BOILERPLATE}"
    exit 1
  else
    printf "CHANGELOG.md release number check: OK ✓\n"
  fi

  if ! date --date="${HEADER_DATE}"
  then
    printf "ERROR invalid commit date (%s) in last CHANGELOG.md entry\n" "$HEADER_DATE"
    printf "Please update the commit date in CHANGELOG.md\n"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  else
    printf "CHANGELOG.md release date check: OK ✓\n"
  fi
}

verify_version(){
  printf "verifying version\n"
  PROJECT_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" "${POM_XML_PATH}")
  printf "Project version from pom.xml is %s\n"  "${PROJECT_VERSION}"

  if [[  "${PROJECT_VERSION}" == *SNAPSHOT ]]
  then
    printf "Version in %s (%s) is a snapshot.\n" "${POM_XML_PATH}" "${PROJECT_VERSION}"
    printf "This script does not release snapshots.\n"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  fi

  if [ "${PROJECT_VERSION}" != "${RELEASE_NUM}" ]
  then
    printf "PROJECT_VERSION %s in pom.xml does not match tag %s" "${PROJECT_VERSION}" "${RELEASE_TAG_NAME}"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  fi

  printf "pom.xml project version (%s) checks with release tag (%s): OK ✓\n" "${PROJECT_VERSION}" "${RELEASE_TAG_NAME}"

  SCM_TAG=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='scm']/*[local-name()='tag']/text()" "${POM_XML_PATH}")

  if [ "${SCM_TAG}" != "${RELEASE_TAG_NAME}" ]
  then
    printf "SCM_TAG %s in pom.xml does not match tag %s\n" "${SCM_TAG}" "${RELEASE_TAG_NAME}"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  fi

}

verify_example_pom(){
  EXAMPLE_DEPENDENCY_VERSION=$(xmllint --xpath "//*[local-name()='dependencies']/*[local-name()='dependency']/*[local-name()='version']/text()" "${EXAMPLE_POM_XML_PATH}")
  if [ "${RELEASE_NUM}" != "${EXAMPLE_DEPENDENCY_VERSION}" ]
  then
    printf "Example dependency version %s does not match the release number %s\n" "${EXAMPLE_DEPENDENCY_VERSION}" "${RELEASE_NUM}"
    printf "Please update the project dependency version in %s\n" "${EXAMPLE_POM_XML_PATH}"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  fi
}

verify_readme(){
  printf "Verifying README %s\n" "${README_PATH}"
  README_NODE_RAW="$(sed -n "/<version>.*<\/version>/p" "${README_PATH}")"

  if [ -z "${README_NODE_RAW}" ]
  then
    printf "Example in %s with <version> tag not found.\n" "${README_PATH}"
    printf "The %s file should include an example with the current release version.\n" "${README_PATH}"
    printf "Exiting...\n"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  fi

  README_NODE="${README_NODE_RAW#"${README_NODE_RAW%%[![:space:]]*}"}"
  README_VERSION="$(echo "${README_NODE}" | sed "s/<version>//" | sed "s/<\/version>//")"

  if [ "${RELEASE_NUM}" != "${README_VERSION}" ]
  then
    VERSION_LINE=$(grep -n "<version>.*</version>" "${README_PATH}" | awk -F '[:]' '{ print $1 }')
    printf "Release tag (%s) does not match example <version> in README.md (%s) on line %s.\n" "${RELEASE_NUM}" "${README_VERSION}" "${VERSION_LINE}"
    printf "Please update README.md to the current release before continuing.\n"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  fi

  printf "Version in README.md (%s) OK ✓.\n" "${README_VERSION}"

  GRADLE_GROOVY_RELEASE="$(grep "implementation \"com.influxdb:influxdb3-java:" "${README_PATH}" | grep -o "[0-9]*\.[0-9]*\.[0-9]*")"

  if [ "${RELEASE_NUM}" != "${GRADLE_GROOVY_RELEASE}"  ]
  then
  GROOVY_LINE=$(grep -n "implementation \"com.influxdb:influxdb3-java:" "${README_PATH}" | awk -F '[:]' '{ print $1 }')
    printf "Release tag (%s) does not match gradle example in README.md (%s) on line %s.\n" "${RELEASE_NUM}" "${GRADLE_GROOVY_RELEASE}" "${GROOVY_LINE}"
    printf "Please update README.md to the current release before continuing.\n"
    printf "%s\n" "${FAILURE_BOILERPLATE}"
    exit 1
  fi
}

echo "Running in GitHub Actions container."

setup

github_check

verify_rc_or_beta
export RC_OR_BETA
set_release_number

verify_version
verify_changelog
verify_example_pom
verify_readme
