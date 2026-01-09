from __future__ import annotations

import main as s3_gfs_main

RetentionPolicy = s3_gfs_main.RetentionPolicy
core_logic = s3_gfs_main.core_logic
apply_removal = s3_gfs_main.apply_removal


def test_retention_policy_expected_decisions():
    # This anchors expected retention outcomes against a real-world file list.
    keys = [
        "Automatic_backup_2025.10.4_2025-11-01_05.20_38003313.metadata.json",
        "Automatic_backup_2025.10.4_2025-11-01_05.20_38003313.tar",
        "Automatic_backup_2025.10.4_2025-11-02_05.10_52126973.metadata.json",
        "Automatic_backup_2025.10.4_2025-11-02_05.10_52126973.tar",
        "Automatic_backup_2025.10.4_2025-11-03_05.38_53003996.metadata.json",
        "Automatic_backup_2025.10.4_2025-11-03_05.38_53003996.tar",
        "Automatic_backup_2025.10.4_2025-11-04_05.04_28012704.metadata.json",
        "Automatic_backup_2025.10.4_2025-11-04_05.04_28012704.tar",
        "Automatic_backup_2025.10.4_2025-11-05_05.17_50003742.metadata.json",
        "Automatic_backup_2025.10.4_2025-11-05_05.17_50003742.tar",
        "Automatic_backup_2025.10.4_2025-11-05_23.39_52434679.metadata.json",
        "Automatic_backup_2025.10.4_2025-11-05_23.39_52434679.tar",
        "Automatic_backup_2025.11.0_2025-11-06_04.49_07006267.metadata.json",
        "Automatic_backup_2025.11.0_2025-11-06_04.49_07006267.tar",
        "Automatic_backup_2025.11.0_2025-11-07_05.31_34003792.metadata.json",
        "Automatic_backup_2025.11.0_2025-11-07_05.31_34003792.tar",
        "Automatic_backup_2025.11.0_2025-11-07_14.13_30096846.metadata.json",
        "Automatic_backup_2025.11.0_2025-11-07_14.13_30096846.tar",
        "Automatic_backup_2025.11.0_2025-11-08_05.12_20004274.metadata.json",
        "Automatic_backup_2025.11.0_2025-11-08_05.12_20004274.tar",
        "Automatic_backup_2025.11.0_2025-11-08_20.12_33781362.metadata.json",
        "Automatic_backup_2025.11.0_2025-11-08_20.12_33781362.tar",
        "Automatic_backup_2025.11.1_2025-11-09_04.45_02004017.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-09_04.45_02004017.tar",
        "Automatic_backup_2025.11.1_2025-11-10_05.03_44004844.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-10_05.03_44004844.tar",
        "Automatic_backup_2025.11.1_2025-11-11_04.56_14003407.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-11_04.56_14003407.tar",
        "Automatic_backup_2025.11.1_2025-11-12_05.41_49003418.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-12_05.41_49003418.tar",
        "Automatic_backup_2025.11.1_2025-11-13_05.17_07005208.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-13_05.17_07005208.tar",
        "Automatic_backup_2025.11.1_2025-11-14_04.52_11004413.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-14_04.52_11004413.tar",
        "Automatic_backup_2025.11.1_2025-11-15_05.19_34003170.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-15_05.19_34003170.tar",
        "Automatic_backup_2025.11.1_2025-11-16_05.22_37003233.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-16_05.22_37003233.tar",
        "Automatic_backup_2025.11.1_2025-11-16_12.02_09201926.metadata.json",
        "Automatic_backup_2025.11.1_2025-11-16_12.02_09201926.tar",
        "Automatic_backup_2025.11.2_2025-11-17_04.57_25004783.metadata.json",
        "Automatic_backup_2025.11.2_2025-11-17_04.57_25004783.tar",
        "Automatic_backup_2025.11.2_2025-11-18_05.33_04003490.metadata.json",
        "Automatic_backup_2025.11.2_2025-11-18_05.33_04003490.tar",
        "Automatic_backup_2025.11.2_2025-11-19_05.05_04015450.metadata.json",
        "Automatic_backup_2025.11.2_2025-11-19_05.05_04015450.tar",
        "Automatic_backup_2025.11.2_2025-11-20_05.33_54004049.metadata.json",
        "Automatic_backup_2025.11.2_2025-11-20_05.33_54004049.tar",
        "Automatic_backup_2025.11.2_2025-11-21_05.02_16009190.metadata.json",
        "Automatic_backup_2025.11.2_2025-11-21_05.02_16009190.tar",
        "Automatic_backup_2025.11.2_2025-11-22_04.56_32002835.metadata.json",
        "Automatic_backup_2025.11.2_2025-11-22_04.56_32002835.tar",
        "Automatic_backup_2025.11.2_2025-11-22_10.10_05992087.metadata.json",
        "Automatic_backup_2025.11.2_2025-11-22_10.10_05992087.tar",
        "Automatic_backup_2025.11.3_2025-11-23_05.44_23009154.metadata.json",
        "Automatic_backup_2025.11.3_2025-11-23_05.44_23009154.tar",
        "Automatic_backup_2025.11.3_2025-11-24_05.16_03003822.metadata.json",
        "Automatic_backup_2025.11.3_2025-11-24_05.16_03003822.tar",
        "Automatic_backup_2025.11.3_2025-11-25_04.48_51003390.metadata.json",
        "Automatic_backup_2025.11.3_2025-11-25_04.48_51003390.tar",
        "Automatic_backup_2025.11.3_2025-11-26_05.00_19004612.metadata.json",
        "Automatic_backup_2025.11.3_2025-11-26_05.00_19004612.tar",
        "Automatic_backup_2025.11.3_2025-11-27_05.11_37003560.metadata.json",
        "Automatic_backup_2025.11.3_2025-11-27_05.11_37003560.tar",
        "Automatic_backup_2025.11.3_2025-11-28_05.38_19003798.metadata.json",
        "Automatic_backup_2025.11.3_2025-11-28_05.38_19003798.tar",
        "Automatic_backup_2025.11.3_2025-11-29_05.43_17003717.metadata.json",
        "Automatic_backup_2025.11.3_2025-11-29_05.43_17003717.tar",
        "Automatic_backup_2025.11.3_2025-11-30_05.24_02003272.metadata.json",
        "Automatic_backup_2025.11.3_2025-11-30_05.24_02003272.tar",
        "Automatic_backup_2025.11.3_2025-12-01_05.23_31003793.metadata.json",
        "Automatic_backup_2025.11.3_2025-12-01_05.23_31003793.tar",
        "Automatic_backup_2025.11.3_2025-12-02_05.43_02003072.metadata.json",
        "Automatic_backup_2025.11.3_2025-12-02_05.43_02003072.tar",
        "Automatic_backup_2025.11.3_2025-12-03_05.10_23005024.metadata.json",
        "Automatic_backup_2025.11.3_2025-12-03_05.10_23005024.tar",
        "Automatic_backup_2025.11.3_2025-12-04_05.38_04004600.metadata.json",
        "Automatic_backup_2025.11.3_2025-12-04_05.38_04004600.tar",
        "Automatic_backup_2025.11.3_2025-12-05_05.20_43003739.metadata.json",
        "Automatic_backup_2025.11.3_2025-12-05_05.20_43003739.tar",
        "Automatic_backup_2025.11.3_2025-12-05_13.19_01874255.metadata.json",
        "Automatic_backup_2025.11.3_2025-12-05_13.19_01874255.tar",
        "Automatic_backup_2025.12.0_2025-12-06_04.48_26004094.metadata.json",
        "Automatic_backup_2025.12.0_2025-12-06_04.48_26004094.tar",
        "Automatic_backup_2025.12.0_2025-12-06_14.39_22475140.metadata.json",
        "Automatic_backup_2025.12.0_2025-12-06_14.39_22475140.tar",
        "Automatic_backup_2025.12.1_2025-12-07_04.57_17003759.metadata.json",
        "Automatic_backup_2025.12.1_2025-12-07_04.57_17003759.tar",
        "Automatic_backup_2025.12.1_2025-12-08_05.25_12003721.metadata.json",
        "Automatic_backup_2025.12.1_2025-12-08_05.25_12003721.tar",
        "Automatic_backup_2025.12.1_2025-12-09_04.58_58004224.metadata.json",
        "Automatic_backup_2025.12.1_2025-12-09_04.58_58004224.tar",
        "Automatic_backup_2025.12.1_2025-12-09_20.27_22890780.metadata.json",
        "Automatic_backup_2025.12.1_2025-12-09_20.27_22890780.tar",
        "Automatic_backup_2025.12.2_2025-12-10_05.16_22003996.metadata.json",
        "Automatic_backup_2025.12.2_2025-12-10_05.16_22003996.tar",
        "Automatic_backup_2025.12.2_2025-12-11_04.53_49028605.metadata.json",
        "Automatic_backup_2025.12.2_2025-12-11_04.53_49028605.tar",
        "Automatic_backup_2025.12.2_2025-12-12_05.25_50004073.metadata.json",
        "Automatic_backup_2025.12.2_2025-12-12_05.25_50004073.tar",
        "Automatic_backup_2025.12.2_2025-12-13_05.18_30004231.metadata.json",
        "Automatic_backup_2025.12.2_2025-12-13_05.18_30004231.tar",
        "Automatic_backup_2025.12.2_2025-12-13_23.00_09357850.metadata.json",
        "Automatic_backup_2025.12.2_2025-12-13_23.00_09357850.tar",
        "Automatic_backup_2025.12.3_2025-12-14_05.09_30003995.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-14_05.09_30003995.tar",
        "Automatic_backup_2025.12.3_2025-12-15_05.10_33004366.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-15_05.10_33004366.tar",
        "Automatic_backup_2025.12.3_2025-12-16_05.02_50004416.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-16_05.02_50004416.tar",
        "Automatic_backup_2025.12.3_2025-12-17_04.52_32004005.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-17_04.52_32004005.tar",
        "Automatic_backup_2025.12.3_2025-12-18_05.15_08012245.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-18_05.15_08012245.tar",
        "Automatic_backup_2025.12.3_2025-12-19_04.48_42004066.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-19_04.48_42004066.tar",
        "Automatic_backup_2025.12.3_2025-12-20_05.22_45004555.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-20_05.22_45004555.tar",
        "Automatic_backup_2025.12.3_2025-12-21_05.06_04003409.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-21_05.06_04003409.tar",
        "Automatic_backup_2025.12.3_2025-12-22_05.39_37003438.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-22_05.39_37003438.tar",
        "Automatic_backup_2025.12.3_2025-12-23_05.18_41003787.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-23_05.18_41003787.tar",
        "Automatic_backup_2025.12.3_2025-12-24_05.16_41003711.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-24_05.16_41003711.tar",
        "Automatic_backup_2025.12.3_2025-12-25_05.29_54003635.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-25_05.29_54003635.tar",
        "Automatic_backup_2025.12.3_2025-12-25_12.28_45671846.metadata.json",
        "Automatic_backup_2025.12.3_2025-12-25_12.28_45671846.tar",
        "Automatic_backup_2025.12.4_2025-12-26_05.21_29004297.metadata.json",
        "Automatic_backup_2025.12.4_2025-12-26_05.21_29004297.tar",
        "Automatic_backup_2025.12.4_2025-12-27_05.29_29005544.metadata.json",
        "Automatic_backup_2025.12.4_2025-12-27_05.29_29005544.tar",
        "Automatic_backup_2025.12.4_2025-12-28_05.11_24003528.metadata.json",
        "Automatic_backup_2025.12.4_2025-12-28_05.11_24003528.tar",
        "Automatic_backup_2025.12.4_2025-12-29_05.19_00003163.metadata.json",
        "Automatic_backup_2025.12.4_2025-12-29_05.19_00003163.tar",
        "Automatic_backup_2025.12.4_2025-12-30_05.17_53062403.metadata.json",
        "Automatic_backup_2025.12.4_2025-12-30_05.17_53062403.tar",
        "Automatic_backup_2025.12.4_2025-12-31_05.06_24004437.metadata.json",
        "Automatic_backup_2025.12.4_2025-12-31_05.06_24004437.tar",
        "Automatic_backup_2025.12.4_2026-01-01_05.21_02003506.metadata.json",
        "Automatic_backup_2025.12.4_2026-01-01_05.21_02003506.tar",
        "Automatic_backup_2025.12.4_2026-01-02_05.44_00008422.metadata.json",
        "Automatic_backup_2025.12.4_2026-01-02_05.44_00008422.tar",
        "Automatic_backup_2025.12.4_2026-01-03_05.22_52003524.metadata.json",
        "Automatic_backup_2025.12.4_2026-01-03_05.22_52003524.tar",
        "Automatic_backup_2025.12.4_2026-01-04_05.10_25003988.metadata.json",
        "Automatic_backup_2025.12.4_2026-01-04_05.10_25003988.tar",
        "Automatic_backup_2025.12.4_2026-01-05_04.53_50003633.metadata.json",
        "Automatic_backup_2025.12.4_2026-01-05_04.53_50003633.tar",
        "Automatic_backup_2025.12.4_2026-01-05_10.49_11079362.metadata.json",
        "Automatic_backup_2025.12.4_2026-01-05_10.49_11079362.tar",
        "Automatic_backup_2025.12.5_2026-01-06_05.17_32890017.metadata.json",
        "Automatic_backup_2025.12.5_2026-01-06_05.17_32890017.tar",
        "Automatic_backup_2025.12.5_2026-01-07_05.22_46003904.metadata.json",
        "Automatic_backup_2025.12.5_2026-01-07_05.22_46003904.tar",
        "Automatic_backup_2025.12.5_2026-01-08_05.22_33003955.metadata.json",
        "Automatic_backup_2025.12.5_2026-01-08_05.22_33003955.tar",
        "Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.metadata.json",
        "Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.tar",
        "Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.metadata.json",
        "Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.tar",
    ]

    policy = RetentionPolicy(
        keep_daily=2,
        keep_weekly=2,
        keep_monthly=2,
    )

    decisions = core_logic(
        keys,
        policy,
        filename_ts_re=s3_gfs_main.re.compile(
            r"Automatic_backup_\d+\.\d+\.\d+_(\d{4}-\d{2}-\d{2}_\d{2}\.\d{2})_"
        ),
        timestamp_format="%Y-%m-%d_%H.%M",
    )

    expected = [
        ("Automatic_backup_2025.10.4_2025-11-01_05.20_38003313.metadata.json", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-01_05.20_38003313.tar", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-02_05.10_52126973.metadata.json", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-02_05.10_52126973.tar", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-03_05.38_53003996.metadata.json", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-03_05.38_53003996.tar", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-04_05.04_28012704.metadata.json", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-04_05.04_28012704.tar", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-05_05.17_50003742.metadata.json", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-05_05.17_50003742.tar", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-05_23.39_52434679.metadata.json", "remove", ""),
        ("Automatic_backup_2025.10.4_2025-11-05_23.39_52434679.tar", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-06_04.49_07006267.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-06_04.49_07006267.tar", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-07_05.31_34003792.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-07_05.31_34003792.tar", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-07_14.13_30096846.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-07_14.13_30096846.tar", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-08_05.12_20004274.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-08_05.12_20004274.tar", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-08_20.12_33781362.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.0_2025-11-08_20.12_33781362.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-09_04.45_02004017.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-09_04.45_02004017.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-10_05.03_44004844.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-10_05.03_44004844.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-11_04.56_14003407.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-11_04.56_14003407.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-12_05.41_49003418.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-12_05.41_49003418.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-13_05.17_07005208.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-13_05.17_07005208.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-14_04.52_11004413.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-14_04.52_11004413.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-15_05.19_34003170.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-15_05.19_34003170.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-16_05.22_37003233.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-16_05.22_37003233.tar", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-16_12.02_09201926.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.1_2025-11-16_12.02_09201926.tar", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-17_04.57_25004783.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-17_04.57_25004783.tar", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-18_05.33_04003490.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-18_05.33_04003490.tar", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-19_05.05_04015450.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-19_05.05_04015450.tar", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-20_05.33_54004049.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-20_05.33_54004049.tar", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-21_05.02_16009190.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-21_05.02_16009190.tar", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-22_04.56_32002835.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-22_04.56_32002835.tar", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-22_10.10_05992087.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.2_2025-11-22_10.10_05992087.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-23_05.44_23009154.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-23_05.44_23009154.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-24_05.16_03003822.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-24_05.16_03003822.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-25_04.48_51003390.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-25_04.48_51003390.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-26_05.00_19004612.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-26_05.00_19004612.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-27_05.11_37003560.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-27_05.11_37003560.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-28_05.38_19003798.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-28_05.38_19003798.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-29_05.43_17003717.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-29_05.43_17003717.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-30_05.24_02003272.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-11-30_05.24_02003272.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-01_05.23_31003793.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-01_05.23_31003793.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-02_05.43_02003072.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-02_05.43_02003072.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-03_05.10_23005024.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-03_05.10_23005024.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-04_05.38_04004600.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-04_05.38_04004600.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-05_05.20_43003739.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-05_05.20_43003739.tar", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-05_13.19_01874255.metadata.json", "remove", ""),
        ("Automatic_backup_2025.11.3_2025-12-05_13.19_01874255.tar", "remove", ""),
        ("Automatic_backup_2025.12.0_2025-12-06_04.48_26004094.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.0_2025-12-06_04.48_26004094.tar", "remove", ""),
        ("Automatic_backup_2025.12.0_2025-12-06_14.39_22475140.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.0_2025-12-06_14.39_22475140.tar", "remove", ""),
        ("Automatic_backup_2025.12.1_2025-12-07_04.57_17003759.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.1_2025-12-07_04.57_17003759.tar", "remove", ""),
        ("Automatic_backup_2025.12.1_2025-12-08_05.25_12003721.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.1_2025-12-08_05.25_12003721.tar", "remove", ""),
        ("Automatic_backup_2025.12.1_2025-12-09_04.58_58004224.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.1_2025-12-09_04.58_58004224.tar", "remove", ""),
        ("Automatic_backup_2025.12.1_2025-12-09_20.27_22890780.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.1_2025-12-09_20.27_22890780.tar", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-10_05.16_22003996.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-10_05.16_22003996.tar", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-11_04.53_49028605.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-11_04.53_49028605.tar", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-12_05.25_50004073.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-12_05.25_50004073.tar", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-13_05.18_30004231.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-13_05.18_30004231.tar", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-13_23.00_09357850.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.2_2025-12-13_23.00_09357850.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-14_05.09_30003995.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-14_05.09_30003995.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-15_05.10_33004366.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-15_05.10_33004366.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-16_05.02_50004416.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-16_05.02_50004416.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-17_04.52_32004005.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-17_04.52_32004005.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-18_05.15_08012245.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-18_05.15_08012245.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-19_04.48_42004066.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-19_04.48_42004066.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-20_05.22_45004555.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-20_05.22_45004555.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-21_05.06_04003409.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-21_05.06_04003409.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-22_05.39_37003438.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-22_05.39_37003438.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-23_05.18_41003787.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-23_05.18_41003787.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-24_05.16_41003711.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-24_05.16_41003711.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-25_05.29_54003635.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-25_05.29_54003635.tar", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-25_12.28_45671846.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.3_2025-12-25_12.28_45671846.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-26_05.21_29004297.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-26_05.21_29004297.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-27_05.29_29005544.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-27_05.29_29005544.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-28_05.11_24003528.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-28_05.11_24003528.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-29_05.19_00003163.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-29_05.19_00003163.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-30_05.17_53062403.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-30_05.17_53062403.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2025-12-31_05.06_24004437.metadata.json", "keep", "monthly"),
        ("Automatic_backup_2025.12.4_2025-12-31_05.06_24004437.tar", "keep", "monthly"),
        ("Automatic_backup_2025.12.4_2026-01-01_05.21_02003506.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-01_05.21_02003506.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-02_05.44_00008422.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-02_05.44_00008422.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-03_05.22_52003524.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-03_05.22_52003524.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-04_05.10_25003988.metadata.json", "keep", "weekly"),
        ("Automatic_backup_2025.12.4_2026-01-04_05.10_25003988.tar", "keep", "weekly"),
        ("Automatic_backup_2025.12.4_2026-01-05_04.53_50003633.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-05_04.53_50003633.tar", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-05_10.49_11079362.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.4_2026-01-05_10.49_11079362.tar", "remove", ""),
        ("Automatic_backup_2025.12.5_2026-01-06_05.17_32890017.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.5_2026-01-06_05.17_32890017.tar", "remove", ""),
        ("Automatic_backup_2025.12.5_2026-01-07_05.22_46003904.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.5_2026-01-07_05.22_46003904.tar", "remove", ""),
        ("Automatic_backup_2025.12.5_2026-01-08_05.22_33003955.metadata.json", "remove", ""),
        ("Automatic_backup_2025.12.5_2026-01-08_05.22_33003955.tar", "remove", ""),
        ("Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.metadata.json", "keep", "daily"),
        ("Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.tar", "keep", "daily"),
        ("Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.metadata.json", "keep", "monthly,weekly,daily"),
        ("Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.tar", "keep", "monthly,weekly,daily"),
    ]
    assert decisions == expected

    kept_keys = {
        "Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.metadata.json",
        "Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.tar",
        "Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.metadata.json",
        "Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.tar",
        "Automatic_backup_2025.12.4_2026-01-04_05.10_25003988.metadata.json",
        "Automatic_backup_2025.12.4_2026-01-04_05.10_25003988.tar",
        "Automatic_backup_2025.12.4_2025-12-31_05.06_24004437.metadata.json",
        "Automatic_backup_2025.12.4_2025-12-31_05.06_24004437.tar",
    }

    removal_result = apply_removal(
        bucket="test-bucket",
        decisions=decisions,
        dry_run=True,
    )

    assert removal_result["deleted"] == 156
    assert len(removal_result["deleted_keys"]) == 156
    # Ensure none of the keepers are scheduled for deletion.
    assert not kept_keys.intersection(removal_result["deleted_keys"])


def test_removal_with_new_backup_only_removes_previous_latest():
    # This checks that when a new backup arrives, the removal logic drops the oldest daily keeper.
    keys = [
        # These are the four kept timestamps from test_retention_policy_expected_decisions.
        "Automatic_backup_2025.12.4_2025-12-31_05.06_24004437.metadata.json",
        "Automatic_backup_2025.12.4_2025-12-31_05.06_24004437.tar",
        "Automatic_backup_2025.12.4_2026-01-04_05.10_25003988.metadata.json",
        "Automatic_backup_2025.12.4_2026-01-04_05.10_25003988.tar",
        "Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.metadata.json",
        "Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.tar",
        "Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.metadata.json",
        "Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.tar",
        # This is the new arrival that should bump the oldest daily keeper.
        "Automatic_backup_2026.1.0_2026-01-10_05.12_12345678.metadata.json",
        "Automatic_backup_2026.1.0_2026-01-10_05.12_12345678.tar",
    ]

    policy = RetentionPolicy(
        keep_daily=2,
        keep_weekly=2,
        keep_monthly=2,
    )

    decisions = core_logic(
        keys,
        policy,
        filename_ts_re=s3_gfs_main.re.compile(
            r"Automatic_backup_\d+\.\d+\.\d+_(\d{4}-\d{2}-\d{2}_\d{2}\.\d{2})_"
        ),
        timestamp_format="%Y-%m-%d_%H.%M",
    )

    removal_result = apply_removal(
        bucket="test-bucket",
        decisions=decisions,
        dry_run=True,
    )

    expected_removed = {
        "Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.metadata.json",
        "Automatic_backup_2025.12.5_2026-01-08_20.47_35897656.tar",
    }

    assert removal_result["deleted"] == 2
    assert set(removal_result["deleted_keys"]) == expected_removed


def test_min_remaining_keeps_five_and_removes_one():
    # This verifies the safety floor prevents deleting below min_remaining.
    keys = [
        "Automatic_backup_2026.01.0_2026-01-01_01.00_00000001.tar",
        "Automatic_backup_2026.01.0_2026-01-02_01.00_00000002.tar",
        "Automatic_backup_2026.01.0_2026-01-03_01.00_00000003.tar",
        "Automatic_backup_2026.01.0_2026-01-04_01.00_00000004.tar",
        "Automatic_backup_2026.01.0_2026-01-05_01.00_00000005.tar",
        "Automatic_backup_2026.01.0_2026-01-06_01.00_00000006.tar",
    ]

    policy = RetentionPolicy(
        keep_daily=2,
        keep_weekly=0,
        keep_monthly=0,
    )

    decisions = core_logic(
        keys,
        policy,
        filename_ts_re=s3_gfs_main.re.compile(
            r"Automatic_backup_\d+\.\d+\.\d+_(\d{4}-\d{2}-\d{2}_\d{2}\.\d{2})_"
        ),
        timestamp_format="%Y-%m-%d_%H.%M",
    )

    removal_result = apply_removal(
        bucket="test-bucket",
        decisions=decisions,
        dry_run=True,
        min_remaining=5,
    )

    # Core marks 2 keeps / 4 removals, but apply_removal should honor min_remaining.
    assert sum(1 for _key, decision, _tag in decisions if decision == "keep") == 2
    assert sum(1 for _key, decision, _tag in decisions if decision == "remove") == 4
    # Only one deletion is allowed so five remain.
    assert removal_result["deleted"] == 1
    assert len(removal_result["deleted_keys"]) == 1
    assert removal_result["deleted_keys"][0] == "Automatic_backup_2026.01.0_2026-01-01_01.00_00000001.tar"


def test_decisions_are_ordered_by_datetime():
    # This confirms decision ordering is oldest-first, which removal relies on.
    keys = [
        "Automatic_backup_2026.01.0_2026-01-02_01.00_00000002.tar",
        "Automatic_backup_2026.01.0_2026-01-01_01.00_00000001.tar",
        "Automatic_backup_2026.01.0_2026-01-03_01.00_00000003.tar",
    ]

    policy = RetentionPolicy(
        keep_daily=0,
        keep_weekly=0,
        keep_monthly=0,
    )

    decisions = core_logic(
        keys,
        policy,
        filename_ts_re=s3_gfs_main.re.compile(
            r"Automatic_backup_\d+\.\d+\.\d+_(\d{4}-\d{2}-\d{2}_\d{2}\.\d{2})_"
        ),
        timestamp_format="%Y-%m-%d_%H.%M",
    )

    assert [key for key, _decision, _tag in decisions] == [
        "Automatic_backup_2026.01.0_2026-01-01_01.00_00000001.tar",
        "Automatic_backup_2026.01.0_2026-01-02_01.00_00000002.tar",
        "Automatic_backup_2026.01.0_2026-01-03_01.00_00000003.tar",
    ]
