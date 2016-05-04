; Installer for Funnel

;======================================================
; Includes

  !include MUI.nsh
  !include Sections.nsh
  !include ..\..\funnel.nsh
  !include envvarupdate.nsh

;======================================================
; Installer Information

  Name "${PROJECT_NAME}"
  SetCompressor /SOLID lzma
  XPStyle on
  CRCCheck on
  InstallDir "C:\Program Files\${PROJECT_ARTIFACT_ID}\"
  AutoCloseWindow false
  ShowInstDetails show
  Icon "${NSISDIR}\Contrib\Graphics\Icons\orange-install.ico"

;======================================================
; Version Tab information for Setup.exe properties

  VIProductVersion ${PROJECT_VERSION}.0
  VIAddVersionKey ProductName "${PROJECT_NAME}.0"
  VIAddVersionKey ProductVersion "${PROJECT_VERSION}"
  VIAddVersionKey CompanyName "${PROJECT_ORGANIZATION_NAME}"
  VIAddVersionKey FileVersion "${PROJECT_VERSION}.0"
  VIAddVersionKey FileDescription ""
  VIAddVersionKey LegalCopyright ""

;======================================================
; Variables


;======================================================
; Modern Interface Configuration

  !define MUI_HEADERIMAGE
  !define MUI_ABORTWARNING
  !define MUI_COMPONENTSPAGE_SMALLDESC
  !define MUI_HEADERIMAGE_BITMAP_NOSTRETCH
  !define MUI_FINISHPAGE
  !define MUI_FINISHPAGE_TEXT "Thank you for installing ${PROJECT_NAME}. \r\n\n\nYou can now run ${PROJECT_ARTIFACT_ID} from your command line."
  !define MUI_ICON "${NSISDIR}\Contrib\Graphics\Icons\orange-install.ico"

;======================================================
; Modern Interface Pages

  !define MUI_DIRECTORYPAGE_VERIFYONLEAVE
;  !insertmacro MUI_PAGE_LICENSE funnel_license.txt
  !insertmacro MUI_PAGE_DIRECTORY
  !insertmacro MUI_PAGE_COMPONENTS
  !insertmacro MUI_PAGE_INSTFILES
  !insertmacro MUI_PAGE_FINISH

;======================================================
; Languages

  !insertmacro MUI_LANGUAGE "English"

;======================================================
; Installer Sections

  !include javadownloader.nsh


Section "FunnelSort"
    SetOutPath $INSTDIR
    SetOverwrite on
    File /r /x *.svn .\programfiles\*
    File /x *source* ..\..\target\funnel-${PROJECT_VERSION}.jar
    File ..\..\..\argument\target\argument-${ARGUMENT_VERSION}.jar
    File ..\..\..\algebrain\target\algebrain-${ALGEBRAIN_VERSION}.jar
    File ..\..\..\calendar\target\calendar-${CALENDAR_VERSION}.jar
    
    FileOpen $9 funnel.bat w
    FileWrite $9 "java -Dversion=${PROJECT_VERSION} -DspecPath=$\"$INSTDIR\scripts$\" -Dlog4j.configuration=$\"$INSTDIR\log4j.xml$\" -jar $\"$INSTDIR\funnel-${PROJECT_VERSION}.jar$\" %*$\r$\n"
    FileClose $9
    
    ${EnvVarUpdate} $0 "PATH" "A" "HKLM" $INSTDIR
    
    writeUninstaller "$INSTDIR\tidy_uninstall.exe"
SectionEnd

; Installer functions
Function .onInstSuccess

FunctionEnd

Section "uninstall"
    ${un.EnvVarUpdate} $0 "PATH" "R" "HKLM" $INSTDIR
    delete $INSTDIR\funnel.bat
    delete $INSTDIR\*.jar
    delete $INSTDIR\log4j.xml
    delete $INSTDIR\commons-csv-1.2.xml
    delete $INSTDIR\funnel.log
SectionEnd

Function .onInit
    InitPluginsDir
FunctionEnd