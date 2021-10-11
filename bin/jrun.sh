#! /bin/bash -u

# mvn -q -f  ~/git/any-db/java/pom.xml install
LIGHT_RED='\033[1;31m'
LIGHT_CYAN='\033[1;36m'
NC='\033[0m' # No Color

function test_availability () {
    type $1 >/dev/null 2>&1 || { echo >&2 -e "${LIGHT_RED}$1 not installed.  Aborting.${NC}"; exit 1; }
}

java_classes_alist=("jrun=JRun"
                    "jcol=JCol"
                    "loadtodb=LoadToDB"
                    "loadrun=LoadRun"
	 	                "sqli=SQLi"
                    "jdbc-server=SQLRemoteServer")

scriptname=$(basename $0)
scriptname=${scriptname%.sh}

for x in "${java_classes_alist[@]}"; do
    y=${x#$scriptname=}
    if [[ $x = "$scriptname=$y" ]]; then
        name=$y
    fi
done

if [ -z "${name+x}" ]; then
    echo "Could not find Java class for script $scriptname"
    exit 1
fi

lname=$(echo "$name" | tr '[:upper:]' '[:lower:]')
thisdir=$(dirname $(readlink -e "${BASH_SOURCE[0]}"))

class=net.inet_lab.any_db.$lname.$name
if [ "$scriptname" = "jdbc-server" ]; then
    class=net.inet_lab.any_db.jdbcserver.$name
fi
cp=/tmp/${class}.cp
if [ -d "$thisdir/../java/$lname" ]; then
    proj="$thisdir/../java/$lname"
elif [ -d "$thisdir/../java/$scriptname" ]; then
    proj="$thisdir/../java/$scriptname"
else
    echo "Cannot find project directory for $scriptname; tried $thisdir/../java/$lname and $thisdir/../java/$scriptname"
    exit 1
fi
jar=$proj/target/$lname-1.0.jar

if ! [ -e "$jar" ]; then
    echo "Jar file $jar not found"
    exit 1
fi
if ! [ -e $cp ] || [ "$jar" -nt "$cp" ]; then
#    if [ -e $cp ]; then
#        ls -l "$jar" "$cp"
#    else
#        echo "No file $cp"
#    fi

    test_availability mvn
    echo mvn -q -f $proj/pom.xml dependency:build-classpath
    rm -f $cp
    mvn -q -f $proj/pom.xml dependency:build-classpath -Dmdep.outputFile=$cp
fi
if [ -n "${JAVA_HOME+x}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    java="$JAVA_HOME/bin/java"
else
    java=java
fi
$java -cp $(cat $cp):$jar $class --default-config leo "$@"
