@echo off
rem Regenerate protocol headers from zfgw.ptl using zgen.
rem Requires zgen in %PATH% (see /zdata/cxxproj/bin/...).

zgen -t header_zds -f * -i zfgw.ptl -o zfgw_proto
zgen -t cpp_zds    -f * -I zfgw_proto.h -I zfgw_pack.h -i zfgw.ptl -o zfgw
pause
