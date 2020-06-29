INCLUDE(FindGit)

IF(GIT_EXECUTABLE)
  EXECUTE_PROCESS(COMMAND "${GIT_EXECUTABLE}" submodule init
                  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")
  EXECUTE_PROCESS(COMMAND "${GIT_EXECUTABLE}" submodule update --remote odbc_gateway
                  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")
ENDIF()
IF(NOT EXISTS ${CMAKE_SOURCE_DIR}/odbc_gateway/pom.xml)
  MESSAGE(FATAL_ERROR "No odbc_gateway! Run
    git submodule init
    git submodule update
Then restart the build.
")
ENDIF()