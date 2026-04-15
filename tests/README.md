### Unit tests (fast, no S3)
python -m pytest -m "not integration" -v

### Integration tests (after uploading sample TIFFs to S3)
RUN_INTEGRATION=1 pytest -m integration -v

### Regenerate golden files after intentional changes
python tests/generate_golden.py
git diff tests/fixtures/expected/

For the integration test: upload 2-3 real sample TIFFs to s3://eawagrs/test/sencast-metadata/tiffs/, run the pipeline once to generate outputs, copy those to tests/fixtures/expected/integration/, and commit them. Future runs will compare against that snapshot.