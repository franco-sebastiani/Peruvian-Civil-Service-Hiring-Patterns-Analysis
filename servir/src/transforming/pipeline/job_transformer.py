"""
Job Transformer - Coordinates all variable transformations for a single job posting.

Calls all predictors and transformers to convert one cleaned job observation
into a fully transformed observation.

Pure business logic - no database I/O (that's handled by orchestrator).
"""

import sys
from pathlib import Path

# Setup imports
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Import all predictors and transformers
from servir.src.transforming.transformers.job_title.job_title_predictor import predict_isco_code
from servir.src.transforming.transformers.institution_name.institution_name_predictor import predict_mef_ids
from servir.src.transforming.transformers.contract_type.contract_transformer import transform_contract
from servir.src.transforming.transformers.experience_requirements.experience_parser import parse_experience
from servir.src.transforming.transformers.academic_profile.academic_parser import parse_academic_profile
from servir.src.transforming.transformers.academic_profile.academic_predictor import predict_program_code


class JobTransformer:
    """Transform a single cleaned job into fully transformed job."""
    
    def __init__(self, 
                 job_title_matches_db,
                 institution_matches_db,
                 academic_matches_db,
                 experience_embedder=None,
                 knowledge_lda=None,
                 competencies_lda=None,
                 specialization_lda=None):
        """
        Initialize transformer with paths to match databases and fitted models.
        
        Args:
            job_title_matches_db (Path): Path to job_title_matches.db
            institution_matches_db (Path): Path to institution_name_matches.db
            academic_matches_db (Path): Path to academic_matches.db
            experience_embedder: Fitted ExperienceEmbedder (optional for now)
            knowledge_lda: Fitted KnowledgeTransformer (optional for now)
            competencies_lda: Fitted CompetenciesTransformer (optional for now)
            specialization_lda: Fitted SpecializationTransformer (optional for now)
        """
        self.job_title_matches_db = job_title_matches_db
        self.institution_matches_db = institution_matches_db
        self.academic_matches_db = academic_matches_db
        self.experience_embedder = experience_embedder
        self.knowledge_lda = knowledge_lda
        self.competencies_lda = competencies_lda
        self.specialization_lda = specialization_lda
    
    def transform(self, cleaned_job_row):
        """
        Transform a single cleaned job observation.
        
        Args:
            cleaned_job_row (dict): One row from cleaned_jobs table
        
        Returns:
            dict: Transformed job with all new columns
        """
        transformed = {**cleaned_job_row}  # Start with all original columns
        
        # 1. Job Title → ISCO
        if cleaned_job_row.get('job_title'):
            isco_prediction = predict_isco_code(
                cleaned_job_row['job_title'],
                self.job_title_matches_db,
                use_model=False
            )
            transformed['isco_code'] = isco_prediction if isco_prediction else None
        else:
            transformed['isco_code'] = None
        
        # 2. Institution → MEF IDs
        if cleaned_job_row.get('institution'):
            mef_prediction = predict_mef_ids(
                cleaned_job_row['institution'],
                self.institution_matches_db,
                use_model=False
            )
            if mef_prediction:
                transformed['ejecutora'] = mef_prediction['ejecutora']
                transformed['ejecutora_nombre'] = mef_prediction['ejecutora_nombre']
                transformed['nivel_gobierno'] = mef_prediction['nivel_gobierno']
                transformed['sector'] = mef_prediction['sector']
                transformed['pliego'] = mef_prediction['pliego']
                transformed['sec_ejec'] = mef_prediction['sec_ejec']
            else:
                transformed.update({
                    'ejecutora': None, 'ejecutora_nombre': None,
                    'nivel_gobierno': None, 'sector': None,
                    'pliego': None, 'sec_ejec': None
                })
        else:
            transformed.update({
                'ejecutora': None, 'ejecutora_nombre': None,
                'nivel_gobierno': None, 'sector': None,
                'pliego': None, 'sec_ejec': None
            })
        
        # 3. Contract → Regime + Temporal Nature
        if cleaned_job_row.get('contract_type'):
            contract_result = transform_contract(cleaned_job_row['contract_type'])
            transformed['contract_regime'] = contract_result['contract_type']
            transformed['contract_temporal_nature'] = contract_result['contract_temporal_nature']
        else:
            transformed['contract_regime'] = None
            transformed['contract_temporal_nature'] = None
        
        # 4. Experience → Parsed fields (+ embeddings if model provided)
        if cleaned_job_row.get('experience_requirements'):
            experience_parsed = parse_experience(cleaned_job_row['experience_requirements'])
            transformed['experience_general_years'] = experience_parsed['experience_general_years']
            transformed['experience_specific_years'] = experience_parsed['experience_specific_years']
            transformed['sector_public_required'] = experience_parsed['sector_public_required']
            transformed['sector_private_required'] = experience_parsed['sector_private_required']
            
            # TODO: Add embedding if embedder provided
            # if self.experience_embedder:
            #     embedding = self.experience_embedder.embed_single(cleaned_job_row['experience_requirements'])
            #     for i, val in enumerate(embedding):
            #         transformed[f'experience_embedding_{i}'] = val
        else:
            transformed.update({
                'experience_general_years': None,
                'experience_specific_years': None,
                'sector_public_required': 0,
                'sector_private_required': 0
            })
        
        # 5. Academic → Program code + Parsed flags
        if cleaned_job_row.get('academic_profile'):
            academic_prediction = predict_program_code(
                cleaned_job_row['academic_profile'],
                self.academic_matches_db,
                use_model=False
            )
            
            if academic_prediction:
                transformed['programa_codigo'] = academic_prediction['programa_codigo']
                transformed['campo_detallado_codigo'] = academic_prediction['campo_detallado_codigo']
                transformed['thesis_required'] = academic_prediction['thesis_required']
            else:
                transformed['programa_codigo'] = None
                transformed['campo_detallado_codigo'] = None
                transformed['thesis_required'] = 0
            
            # Parse additional flags
            academic_parsed = parse_academic_profile(cleaned_job_row['academic_profile'])
            transformed['accepts_related_fields'] = academic_parsed['accepts_related_fields']
            transformed['requires_colegiado'] = academic_parsed['requires_colegiado']
            transformed['requires_habilitado'] = academic_parsed['requires_habilitado']
        else:
            transformed.update({
                'programa_codigo': None,
                'campo_detallado_codigo': None,
                'thesis_required': 0,
                'accepts_related_fields': 0,
                'requires_colegiado': 0,
                'requires_habilitado': 0
            })
        
        # 6-8. Knowledge, Competencies, Specialization → LDA topics
        # TODO: Implement when LDA models are fitted
        # For now, skip these (will be added by orchestrator after fitting LDA)
        
        return transformed