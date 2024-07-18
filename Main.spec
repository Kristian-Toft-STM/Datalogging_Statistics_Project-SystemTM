# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\Main.py'],
    pathex=[],
    binaries=[],
    datas=[('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\json_functions.py', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\Misc.py', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\OPCUA_Functions.py', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\opti-track.db', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\previous_setup_step7.json', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\setup_step7.json', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\snap7.dll', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\Snap7_Functions.py', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\SQLiteRead.py', '.'), ('C:\\Users\\STM\\Desktop\\Praktik_Eksamensprojekt\\Datalogging_Statistics_Project-SystemTM\\SQLiteWrite.py', '.')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Main',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Main',
)
