# Regulations.gov Attachments API Notes

## Candidate Endpoints

- `GET /comments/{commentId}/attachments`
- `GET /attachments/{attachmentId}`

Base URL defaults to `https://api.regulations.gov/v4`.

## Expected Metadata

Attachment resources may expose file metadata through JSON:API-style `data`
items. Common fields include:

- `id`
- `attributes.title`
- `attributes.fileUrl`
- `attributes.format`
- `attributes.size`
- `attributes.contentType`

Provider payloads can vary. This fetch skill preserves raw metadata and records
missing `fileUrl` as a limitation rather than treating it as absence of content.
