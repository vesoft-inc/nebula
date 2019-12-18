# Nebula JNI codec library
It supports encode/decode data in nebula graph.


# How to build

## Build requirements
To build this project, you must have:
  * Access to the Internet
  * make
  * cmake 3.0.0+
  * GCC 6.0+
  * glog


## Steps
 * mkdir build && cd build
 * cmake .. -DNEBULA_HOME=${nebula project root dir} -DNEBULA_THIRDPARTY_ROOT=${dependencies root dir}
 * make
 * cd ../java && mvn clean package

You could find the jni java package nebula-utils-1.0.0-beta.jar under java/target dir
