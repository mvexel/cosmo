use osmpbf::{DenseNodeInfo, Info};
use std::sync::atomic::{AtomicU64, Ordering};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElementType {
    Node,
    Way,
    Relation,
}

impl ElementType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Way => "way",
            Self::Relation => "relation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataFieldKind {
    Integer,
    String,
    Boolean,
    Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataField {
    Id,
    Type,
    Visible,
    Version,
    Changeset,
    Timestamp,
    Uid,
    User,
}

impl MetadataField {
    pub const SUPPORTED: &'static str =
        "id, type, visible, version, changeset, timestamp, uid, user";

    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "id" => Ok(Self::Id),
            "type" => Ok(Self::Type),
            "visible" => Ok(Self::Visible),
            "version" => Ok(Self::Version),
            "changeset" => Ok(Self::Changeset),
            "timestamp" => Ok(Self::Timestamp),
            "uid" => Ok(Self::Uid),
            "user" => Ok(Self::User),
            _ => Err(format!(
                "Unknown metadata field 'meta:{name}'; supported fields: {}",
                Self::SUPPORTED
            )),
        }
    }

    pub fn kind(self) -> MetadataFieldKind {
        match self {
            Self::Id | Self::Version | Self::Changeset | Self::Uid => MetadataFieldKind::Integer,
            Self::Type | Self::User => MetadataFieldKind::String,
            Self::Visible => MetadataFieldKind::Boolean,
            Self::Timestamp => MetadataFieldKind::Timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetadataFields {
    pub id: i64,
    pub element_type: ElementType,
    pub visible: Option<bool>,
    pub version: Option<i64>,
    pub changeset: Option<i64>,
    pub timestamp_millis: Option<i64>,
    pub uid: Option<i64>,
    pub user: Option<String>,
}

#[derive(Default)]
pub struct MetadataAvailability {
    inspected: AtomicU64,
    with_timestamp: AtomicU64,
    with_user_name: AtomicU64,
    with_nonzero_uid: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataAvailabilitySnapshot {
    pub inspected: u64,
    pub with_timestamp: u64,
    pub with_user_name: u64,
    pub with_nonzero_uid: u64,
}

impl MetadataAvailability {
    pub fn record(&self, metadata: Option<&MetadataFields>) {
        self.inspected.fetch_add(1, Ordering::Relaxed);

        let Some(metadata) = metadata else {
            return;
        };

        if metadata.timestamp_millis.is_some() {
            self.with_timestamp.fetch_add(1, Ordering::Relaxed);
        }
        if metadata
            .user
            .as_deref()
            .is_some_and(|user| !user.is_empty())
        {
            self.with_user_name.fetch_add(1, Ordering::Relaxed);
        }
        if metadata.uid.is_some_and(|uid| uid != 0) {
            self.with_nonzero_uid.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> MetadataAvailabilitySnapshot {
        MetadataAvailabilitySnapshot {
            inspected: self.inspected.load(Ordering::Relaxed),
            with_timestamp: self.with_timestamp.load(Ordering::Relaxed),
            with_user_name: self.with_user_name.load(Ordering::Relaxed),
            with_nonzero_uid: self.with_nonzero_uid.load(Ordering::Relaxed),
        }
    }
}

impl MetadataAvailabilitySnapshot {
    pub fn has_stripped_user_names(self) -> bool {
        self.with_timestamp > 0 && self.with_user_name == 0
    }

    pub fn has_stripped_user_ids(self) -> bool {
        self.with_timestamp > 0 && self.with_nonzero_uid == 0
    }
}

pub fn build_metadata_from_info(id: i64, element_type: ElementType, info: &Info) -> MetadataFields {
    MetadataFields {
        id,
        element_type,
        visible: Some(info.visible()),
        version: info.version().map(i64::from),
        changeset: info.changeset(),
        timestamp_millis: info.milli_timestamp(),
        uid: info.uid().map(i64::from),
        user: info
            .user()
            .and_then(|user| user.ok())
            .map(|s| s.to_string()),
    }
}

pub fn build_metadata_from_dense_info(id: i64, info: &DenseNodeInfo) -> MetadataFields {
    MetadataFields {
        id,
        element_type: ElementType::Node,
        visible: Some(info.visible()),
        version: Some(i64::from(info.version())),
        changeset: Some(info.changeset()),
        timestamp_millis: Some(info.milli_timestamp()),
        uid: Some(i64::from(info.uid())),
        user: info.user().ok().map(|s| s.to_string()),
    }
}

pub fn format_timestamp_millis(millis: i64) -> Option<String> {
    let nanos = i128::from(millis) * 1_000_000;
    let dt = OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?;
    dt.format(&Rfc3339).ok()
}

pub fn metadata_value_string(
    field: MetadataField,
    metadata: Option<&MetadataFields>,
) -> Option<String> {
    let meta = metadata?;
    match field {
        MetadataField::Id => Some(meta.id.to_string()),
        MetadataField::Type => Some(meta.element_type.as_str().to_string()),
        MetadataField::Visible => meta.visible.map(|v| v.to_string()),
        MetadataField::Version => meta.version.map(|v| v.to_string()),
        MetadataField::Changeset => meta.changeset.map(|v| v.to_string()),
        MetadataField::Timestamp => meta.timestamp_millis.and_then(format_timestamp_millis),
        MetadataField::Uid => meta.uid.map(|v| v.to_string()),
        MetadataField::User => meta.user.clone(),
    }
}

pub fn metadata_integer_value(
    field: MetadataField,
    metadata: Option<&MetadataFields>,
) -> Option<i64> {
    let meta = metadata?;
    match field {
        MetadataField::Id => Some(meta.id),
        MetadataField::Version => meta.version,
        MetadataField::Changeset => meta.changeset,
        MetadataField::Uid => meta.uid,
        _ => None,
    }
}

pub fn metadata_value_exists(field: MetadataField, metadata: Option<&MetadataFields>) -> bool {
    match field {
        MetadataField::Id | MetadataField::Type => metadata.is_some(),
        MetadataField::Visible => metadata.and_then(|m| m.visible).is_some(),
        MetadataField::Version => metadata.and_then(|m| m.version).is_some(),
        MetadataField::Changeset => metadata.and_then(|m| m.changeset).is_some(),
        MetadataField::Timestamp => metadata.and_then(|m| m.timestamp_millis).is_some(),
        MetadataField::Uid => metadata.and_then(|m| m.uid).is_some(),
        MetadataField::User => metadata.and_then(|m| m.user.as_deref()).is_some(),
    }
}

pub fn extract_meta_value(key: &str, metadata: Option<&MetadataFields>) -> Option<String> {
    MetadataField::parse(key)
        .ok()
        .and_then(|field| metadata_value_string(field, metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> MetadataFields {
        MetadataFields {
            id: 42,
            element_type: ElementType::Way,
            visible: Some(true),
            version: Some(3),
            changeset: Some(99),
            timestamp_millis: Some(1_704_067_200_000),
            uid: Some(7),
            user: Some("Jane Doe".to_string()),
        }
    }

    #[test]
    fn extracts_supported_metadata_values() {
        let meta = metadata();

        assert_eq!(extract_meta_value("id", Some(&meta)).as_deref(), Some("42"));
        assert_eq!(
            extract_meta_value("type", Some(&meta)).as_deref(),
            Some("way")
        );
        assert_eq!(
            extract_meta_value("visible", Some(&meta)).as_deref(),
            Some("true")
        );
        assert_eq!(
            extract_meta_value("version", Some(&meta)).as_deref(),
            Some("3")
        );
        assert_eq!(
            extract_meta_value("changeset", Some(&meta)).as_deref(),
            Some("99")
        );
        assert_eq!(
            extract_meta_value("timestamp", Some(&meta)).as_deref(),
            Some("2024-01-01T00:00:00Z")
        );
        assert_eq!(extract_meta_value("uid", Some(&meta)).as_deref(), Some("7"));
        assert_eq!(
            extract_meta_value("user", Some(&meta)).as_deref(),
            Some("Jane Doe")
        );
    }

    #[test]
    fn rejects_unknown_metadata_field() {
        let err = MetadataField::parse("unknown").unwrap_err();

        assert!(err.contains("Unknown metadata field 'meta:unknown'"));
        assert!(err.contains(MetadataField::SUPPORTED));
        assert_eq!(extract_meta_value("unknown", Some(&metadata())), None);
    }

    #[test]
    fn detects_stripped_user_names_when_timestamps_are_present() {
        let stats = MetadataAvailability::default();
        let mut meta = metadata();
        meta.user = Some(String::new());
        meta.uid = Some(7);

        stats.record(Some(&meta));

        assert!(stats.snapshot().has_stripped_user_names());
        assert!(!stats.snapshot().has_stripped_user_ids());
    }

    #[test]
    fn detects_stripped_user_ids_when_timestamps_are_present() {
        let stats = MetadataAvailability::default();
        let mut meta = metadata();
        meta.uid = Some(0);

        stats.record(Some(&meta));

        assert!(stats.snapshot().has_stripped_user_ids());
        assert!(!stats.snapshot().has_stripped_user_names());
    }
}
