from __future__ import annotations

# Annotation table DDL
CREATE_ANNOTATION_TABLE = """
CREATE TABLE IF NOT EXISTS attendance_annotations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `date` DATE NOT NULL,
    annotation_type ENUM('Open Late', 'Early Close', 'Special Hours', 'Full Off') NOT NULL,
    reason TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_annotation_date (`date`)
)
"""

# Annotation DML (prepared statements only)
UPSERT_ANNOTATION = """
INSERT INTO attendance_annotations (`date`, annotation_type, reason)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
    annotation_type = VALUES(annotation_type),
    reason = VALUES(reason),
    created_at = CURRENT_TIMESTAMP
"""

DELETE_ANNOTATION_BY_DATE = """
DELETE FROM attendance_annotations
WHERE `date` = %s
"""

SELECT_ANNOTATIONS = """
SELECT `date`, annotation_type, reason, created_at
FROM attendance_annotations
ORDER BY `date`
"""

HEALTH_CHECK = "SELECT 1"

# Suggested production indexes (run in migration tooling as needed):
# CREATE INDEX idx_annotations_type_date ON attendance_annotations(annotation_type, `date`);
