#!/bin/bash -e

# Builds the Union distribution of Couchbase Lite iOS: release-optimised static frameworks
# packaged as XCFrameworks (ios-arm64 device, ios-arm64_x86_64-simulator), the same product
# layout as the couchbase-lite-ios CocoaPod that POS/KDS have shipped since 1.4.4.
#
# Usage:
#   BuildScripts/build_union_frameworks.sh
#
# Environment:
#   OUT_DIR                        output root (default: build/union)
#   UPSTREAM_COUCHBASELITE_BINARY  upstream 1.4.4 CouchbaseLite static framework binary used by the
#                                  verification gate (default: ../pos/Pods/couchbase-lite-ios/...)
#
# Output (under OUT_DIR):
#   xcframeworks/CouchbaseLite.xcframework
#   xcframeworks/CouchbaseLiteListener.xcframework
#   couchbase-lite-ios-<version>.zip     both XCFrameworks + LICENSE.txt
#   <sdk>/build.log                      xcodebuild logs, one per SDK
#
# The frameworks are static archives (STRIP_INSTALLED_PRODUCT=NO keeps DWARF in the object files),
# so there are no separate dSYMs: CBL symbols end up in the dSYM of the app that links them.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/build/union}"
UPSTREAM_COUCHBASELITE_BINARY="${UPSTREAM_COUCHBASELITE_BINARY:-$ROOT/../pos/Pods/couchbase-lite-ios/CouchbaseLite.framework/CouchbaseLite}"

PROJECT="$ROOT/CouchbaseLite.xcodeproj"
TARGETS=(-target "CBL iOS" -target "CBL Listener iOS")
PRODUCTS=(CouchbaseLite CouchbaseLiteListener)
DEPLOYMENT_TARGET=15.0

cd "$ROOT"

VERSION="$(sed -n 's/^CBL_VERSION_STRING[[:space:]]*=[[:space:]]*//p' xcconfigs/Project.xcconfig | tr -d '[:space:]')"
SOURCE_REVISION="$(git rev-parse --short HEAD)"
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    SOURCE_REVISION="${SOURCE_REVISION}+CHANGES"
fi
BUILD_NUMBER="$(git rev-list --count HEAD)"

fail() {
    echo "VERIFICATION FAILED: $*" >&2
    exit 1
}

echo "Building Couchbase Lite ${VERSION} (${SOURCE_REVISION}, build ${BUILD_NUMBER}) into ${OUT_DIR}"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

build_sdk() {
    local sdk="$1" archs="$2"
    local sdk_dir="$OUT_DIR/$sdk"
    mkdir -p "$sdk_dir"
    echo "== xcodebuild ${sdk} (${archs})"
    # ALREADYINVOKED stops BuildFatLibrary.sh from recursing into the other SDK and lipo-ing device
    # and simulator slices together, which is impossible once both are arm64.
    ALREADYINVOKED=true xcodebuild build \
        -project "$PROJECT" "${TARGETS[@]}" \
        -configuration Release -sdk "$sdk" \
        ARCHS="$archs" ONLY_ACTIVE_ARCH=NO \
        IPHONEOS_DEPLOYMENT_TARGET="$DEPLOYMENT_TARGET" \
        ENABLE_BITCODE=NO BITCODE_GENERATION_MODE= \
        GCC_TREAT_WARNINGS_AS_ERRORS=NO \
        CODE_SIGNING_ALLOWED=NO \
        CBL_VERSION_STRING="$VERSION" CBL_SOURCE_REVISION="$SOURCE_REVISION" \
        CURRENT_PROJECT_VERSION="$BUILD_NUMBER" \
        SYMROOT="$sdk_dir/sym" OBJROOT="$sdk_dir/obj" \
        > "$sdk_dir/build.log" 2>&1 || { tail -n 40 "$sdk_dir/build.log"; fail "xcodebuild ${sdk} failed, see $sdk_dir/build.log"; }
    for product in "${PRODUCTS[@]}"; do
        local framework="$sdk_dir/sym/Release-iOS-universal/${product}.framework"
        [[ -f "$framework/$product" ]] || fail "missing $framework/$product"
        rm -rf "$framework/PrivateHeaders"
    done
}

build_sdk iphoneos "arm64"
build_sdk iphonesimulator "arm64 x86_64"

XCFRAMEWORKS_DIR="$OUT_DIR/xcframeworks"
mkdir -p "$XCFRAMEWORKS_DIR"
for product in "${PRODUCTS[@]}"; do
    echo "== xcodebuild -create-xcframework ${product}"
    xcodebuild -create-xcframework \
        -framework "$OUT_DIR/iphoneos/sym/Release-iOS-universal/${product}.framework" \
        -framework "$OUT_DIR/iphonesimulator/sym/Release-iOS-universal/${product}.framework" \
        -output "$XCFRAMEWORKS_DIR/${product}.xcframework" > /dev/null
done

echo "== Verification"

slice_binary() {
    echo "$XCFRAMEWORKS_DIR/$1.xcframework/$2/$1.framework/$1"
}

for product in "${PRODUCTS[@]}"; do
    xcframework="$XCFRAMEWORKS_DIR/${product}.xcframework"
    identifiers="$(plutil -extract AvailableLibraries json -o - "$xcframework/Info.plist" \
        | python3 -c 'import json,sys; print(" ".join(sorted(l["LibraryIdentifier"] for l in json.load(sys.stdin))))')"
    [[ "$identifiers" == "ios-arm64 ios-arm64_x86_64-simulator" ]] \
        || fail "${product}.xcframework slices are '${identifiers}'"

    device="$(slice_binary "$product" ios-arm64)"
    simulator="$(slice_binary "$product" ios-arm64_x86_64-simulator)"
    [[ "$(lipo -archs "$device")" == "arm64" ]] || fail "${product} device slice archs: $(lipo -archs "$device")"
    [[ "$(lipo -archs "$simulator" | tr ' ' '\n' | sort | tr '\n' ' ')" == "arm64 x86_64 " ]] \
        || fail "${product} simulator slice archs: $(lipo -archs "$simulator")"

    for pair in "$device:arm64:2" "$simulator:arm64:7" "$simulator:x86_64:7"; do
        IFS=: read -r binary arch platform <<< "$pair"
        load_commands="$(otool -arch "$arch" -l "$binary" | awk '/LC_BUILD_VERSION/{f=1} f&&/platform/{p=$2} f&&/minos/{print p, $2; f=0}' | sort -u)"
        [[ "$load_commands" == "$platform $DEPLOYMENT_TARGET" ]] \
            || fail "${product} ${arch} LC_BUILD_VERSION (platform minos) = '${load_commands}', expected '${platform} ${DEPLOYMENT_TARGET}'"
    done

    [[ -d "$xcframework/ios-arm64/${product}.framework/PrivateHeaders" ]] && fail "${product} still ships PrivateHeaders"
    [[ -n "$(ls "$xcframework/ios-arm64/${product}.framework/Headers")" ]] || fail "${product} has no public headers"
done

for sdk in iphoneos iphonesimulator; do
    log="$OUT_DIR/$sdk/build.log"
    grep -q -- ' -Os ' "$log" || fail "$sdk build log has no -Os compile"
    grep -q -- ' -O0 ' "$log" && fail "$sdk build log contains an -O0 compile"
done

device_cbl="$(slice_binary CouchbaseLite ios-arm64)"
strings -arch arm64 "$device_cbl" | grep -q -F -- "$VERSION" || fail "version string ${VERSION} not found in device slice"
strings -arch arm64 "$device_cbl" | grep -q -F -- "${VERSION}-Debug" && fail "device slice carries the Debug version string"
nm -arch arm64 "$device_cbl" | grep -q -F '_OBJC_CLASS_$_CBL_SQLiteStorage' || fail "SQLite storage is not linked into CouchbaseLite"
nm -arch arm64 "$(slice_binary CouchbaseLiteListener ios-arm64)" | grep -q -F '_OBJC_CLASS_$_CBLListener' || fail "CBLListener is not in CouchbaseLiteListener"

[[ -f "$UPSTREAM_COUCHBASELITE_BINARY" ]] || fail "upstream binary not found at $UPSTREAM_COUCHBASELITE_BINARY (set UPSTREAM_COUCHBASELITE_BINARY)"
compare_dir="$OUT_DIR/compare"
mkdir -p "$compare_dir"
lipo -thin arm64 "$UPSTREAM_COUCHBASELITE_BINARY" -output "$compare_dir/upstream_arm64.a"

objc_classes() {
    nm -arch arm64 -gU "$1" | awk '{print $NF}' | grep -F '_OBJC_CLASS_$_' | grep -v -F 'METACLASS' | sort -u
}
class_diff="$(diff <(objc_classes "$compare_dir/upstream_arm64.a") <(objc_classes "$device_cbl") || true)"
[[ -z "$class_diff" ]] || fail "Objective-C class set differs from upstream:
$class_diff"

text_size() {
    size -arch arm64 "$1" | awk 'NR > 1 && $1 ~ /^[0-9]+$/ { total += $1 } END { print total }'
}
upstream_text="$(text_size "$compare_dir/upstream_arm64.a")"
device_text="$(text_size "$device_cbl")"
ratio_ok="$(python3 -c "print(int(0.70 <= $device_text / $upstream_text <= 1.15))")"
[[ "$ratio_ok" == "1" ]] || fail "device __TEXT size ${device_text} is outside 70%-115% of upstream ${upstream_text}"
echo "device __TEXT ${device_text} bytes vs upstream ${upstream_text} bytes"

ZIP_NAME="couchbase-lite-ios-${VERSION}.zip"
cp "$ROOT/LICENSE" "$XCFRAMEWORKS_DIR/LICENSE.txt"
(cd "$XCFRAMEWORKS_DIR" && rm -f "../$ZIP_NAME" && zip -q -r -y "../$ZIP_NAME" CouchbaseLite.xcframework CouchbaseLiteListener.xcframework LICENSE.txt)

echo "OK: $OUT_DIR/$ZIP_NAME"
