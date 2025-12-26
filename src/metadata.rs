use osmpbf::{DenseNodeInfo, Info};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub struct MetadataFields {
    pub id: i64,
    pub visible: Option<bool>,
    pub version: Option<i64>,
    pub changeset: Option<i64>,
    pub timestamp: Option<String>,
    pub uid: Option<i64>,
    pub user: Option<String>,
}

pub fn build_metadata_from_info(id: i64, info: &Info) -> MetadataFields {
    MetadataFields {
        id,
        visible: Some(info.visible()),
        version: info.version().map(i64::from),
        changeset: info.changeset(),
        timestamp: info.milli_timestamp().and_then(format_timestamp_millis),
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
        visible: Some(info.visible()),
        version: Some(i64::from(info.version())),
        changeset: Some(info.changeset()),
        timestamp: format_timestamp_millis(info.milli_timestamp()),
        uid: Some(i64::from(info.uid())),
        user: info.user().ok().map(|s| s.to_string()),
    }
}

pub fn format_timestamp_millis(millis: i64) -> Option<String> {
    let nanos = i128::from(millis) * 1_000_000;
    let dt = OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?;
    dt.format(&Rfc3339).ok()
}

pub fn extract_meta_value(key: &str, metadata: Option<&MetadataFields>) -> Option<String> {
    let meta = metadata?;
    match key {
        "id" => Some(meta.id.to_string()),
        "visible" => meta.visible.map(|v| v.to_string()),
        "version" => meta.version.map(|v| v.to_string()),
        "changeset" => meta.changeset.map(|v| v.to_string()),
        "timestamp" => meta.timestamp.clone(),
        "uid" => meta.uid.map(|v| v.to_string()),
        "user" => meta.user.clone(),
        _ => None,
    }
}
