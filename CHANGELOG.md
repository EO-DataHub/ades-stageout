# Changelog

## v0.1.4 (2024-11-06)
Update asset handling to maintain external links
- Separate out the handling of asset hrefs to avoid duplicate code
- Ensure asset hrefs that are external to our deployment are left as found and are not updated to use HTTPS links
- Update unit tests to confirm this

## v0.1.3 (2024-10-31
Support Annotations and Pre-generated Collections
- Extract annotation files (`*-annotations.json`) and send to transformer
- Handle collections contained within workflow outputs
  - In this case no new collection needs to be generated
  - Links are updated to ensure absolute links based on HTTPS S3 locations)

## v0.1.2 (2024-10-29)
Support collections output by workflows
- Check for collections generated in workflow outputs
- Update links in the catalog, collections and items to be https links
- Upload files to S3 just as with newly generated collections

## v0.1.1 (2024-08-07)
Bug fixes:
- Output HREFs are set separately to match the structure required for nested catalogs


## v0.1.0 (2024-07-17)
Initial commit:
- Add stageout.py code to separate repository
- Restructuring code from EOEPCA to support better testing
- Provide Dockerfile to build eodhp-ades-stageout image
- Include unit tests


