from metadata.ingestion.ometa.ometa_api import OpenMetadata

class WorkflowContext:
    def __init__(self, metadata: OpenMetadata, pipeline_name: str, workflow_config):
        self.metadata = metadata
        self.pipeline_name = pipeline_name
        self.config = workflow_config
