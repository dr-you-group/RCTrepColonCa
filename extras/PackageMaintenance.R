# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of RCTrepColonCa
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#Replace the name of the package
fileList <- list.files(recursive = T)
for (aFile in fileList) {
  aFileText <- readLines(aFile)
  modified_text <- gsub('RCTrepColonCa', 'RCTrepColonCa', aFileText)
  writeLines(modified_text, aFile)
}

# Format and check code ---------------------------------------------------
styler::style_pkg()
OhdsiRTools::checkUsagePackage("RCTrepColonCa")
OhdsiRTools::updateCopyrightYearFolder()

# Create manual -----------------------------------------------------------
unlink("extras/UsingSkeletonPackage.pdf")
shell("R CMD Rd2pdf ./ --output=extras/UsingSkeletonPackage.pdf")

# Store environment in which the study was executed -----------------------
OhdsiRTools::createRenvLockFile(rootPackage = "RCTrepColonCa",
                                mode = "description",
                                ohdsiGitHubPackages = unique(c(OhdsiRTools::getOhdsiGitHubPackages())),
                                includeRootPackage = FALSE)




