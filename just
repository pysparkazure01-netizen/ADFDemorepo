Why Joining on HB_ALERTS_ID is Wrong:
      • It changes every time you insert a new alert.
      • A re-trigger of the same event will not match the old alert.
      • Migrations, backfills, or reloads will break because sequence numbers differ.
      • Surrogate keys cannot be trusted across environments (DEV ® QA ® PROD).
Example (Wrong):
        Assume the same alert triggers twice.
        Run 1:
        HB_ALERTS_ID = 501
        Run 2:
        HB_ALERTS_ID = 850
If you join on HB_ALERTS_ID, the second run will NOT match the first one ®
You will accidentally insert duplicates.

Why Joining on SRC_UNIQ_CD is Correct:

SRC_UNIQ_CD remains constant for the same real-world alert.
So when an alert reappears:
Run 1:
SRC_UNIQ_CD = 6_EXT-PMT-0002_12345_202411
Run 2:
SRC_UNIQ_CD = 6_EXT-PMT-0002_12345_202411
Both runs match ® the system identifies it as the same alert.
