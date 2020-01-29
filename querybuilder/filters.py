import urllib

from django.conf import settings
from django.db.models import QuerySet, Q

from param_forms.views import FormsFilterViewSet
from querybuilder.param_filters import QueryFilterBackend, WhereFilterBackend, SkillsWhereFilter, LocationQueryFilter, \
    HiringEventQueryFilter, ExperienceQueryFilter, GenderQueryFilter, AgeQueryFilter, CompletedTenureQueryFilter, \
    JobLevelQueryFilter, PerformanceRatingQueryFilter, CurrentTenureRatingQueryFilter
from querybuilder.query import Query

from querybuilder.helpers import decode_complex_ops
from querybuilder.join_helpers import CandidateDataViewJoin, PredictedScoreJoin, JoinHandlerBackend
from sourcing import models as sourcing_models
from querybuilder import join_helpers
from sourcing.filters import parse_form_filter_params


def candidates_filter(request, job_uid):
    query_string = request.GET.get('filters', '')
    page_number = int(request.GET.get('page', 1))
    custom_string = request.GET.get('custom_filters', '')
    form_filter = request.GET.get('form_filter', '')

    join_attrs = {
        'job_uid': job_uid,
    }
    print([func(request, **join_attrs).check for func in [PredictedScoreJoin, CandidateDataViewJoin]])
    join_backend = JoinHandlerBackend(join_helpers.list_of_joins, [func(request, **join_attrs) for func in [PredictedScoreJoin, CandidateDataViewJoin]])
    candidates_query = Query().from_table(sourcing_models.Candidate)
    candidates_query = join_backend.apply(candidates_query, query_string)

    encoded_query_string = urllib.quote_plus(query_string)
    query_filters = decode_complex_ops(encoded_query_string)
    query_filterset = [LocationQueryFilter, HiringEventQueryFilter, ExperienceQueryFilter, GenderQueryFilter, AgeQueryFilter, CompletedTenureQueryFilter, JobLevelQueryFilter, PerformanceRatingQueryFilter, CurrentTenureRatingQueryFilter]
    query_filter_backend = QueryFilterBackend(query_filterset)
    candidates_query = query_filter_backend.apply_filters(request, job_uid, candidates_query, query_filters)
    if job_uid:
        candidates_query = candidates_query.where(Q(**{'sourcing_application.job_id': job_uid}))

    encoded_custom_string = urllib.quote_plus(custom_string)
    custom_filters = decode_complex_ops(encoded_custom_string)
    where_backend = WhereFilterBackend([SkillsWhereFilter])
    custom_where = where_backend.apply_filters(custom_filters)

    form_filter_vs_object = FormsFilterViewSet()
    form_filter_list = form_filter.split(",")
    for filter_string in form_filter_list:
        question_id, value, operator, addition_condition = parse_form_filter_params(filter_string)
        # Check for the 3 necessary params for form filter additional_condition is optional and can be None
        if question_id and value and operator:
            form_filter_applied_candidate_id = form_filter_vs_object.filter_response(question_id, value, operator,
                                                                                     addition_condition)
            candidates_query.where(Q(**{'id__in': form_filter_applied_candidate_id}))

    designation_list = candidates_query.get_sql(custom_where=custom_where, custom_select='DISTINCT employee_employee.designation', replace_select=True)
    locations_list = candidates_query.get_sql(custom_where=custom_where,
                                                custom_select="DISTINCT candidate_data.other_fields ->> 'location'",
                                                replace_select=True)
    tags_list = candidates_query.get_sql(custom_where=custom_where,
                                           custom_select='DISTINCT sourcing_tag.title',
                                           replace_select=True)
    status_list = candidates_query.get_sql(custom_where=custom_where,
                                           custom_select='DISTINCT sourcing_applicationcategory.title',
                                           replace_select=True)
    added_by_list = candidates_query.get_sql(custom_where=custom_where,
                                           custom_select='DISTINCT uploader_email',
                                           replace_select=True)
    source_list = candidates_query.get_sql(custom_where=custom_where,
                                           custom_select='DISTINCT sourcing_candidatesourcetype.source_type',
                                           replace_select=True)

    page_size = settings.REST_FRAMEWORK['PAGE_SIZE']
    offset = (page_number - 1) * page_size
    candidates_query = candidates_query.limit(page_size, offset)
    query = candidates_query.get_sql(custom_where=custom_where,
        custom_select='distinct on (COALESCE(sourcing_candidate.email_id, sourcing_candidate.id::text)) sourcing_predictedscore.score as score, sourcing_applicationstatus.stage as application_status, sourcing_application.verified as verified, sourcing_application.other_fields as application_fields, sourcing_application.application_form_response as application_form_status, sourcing_candidatesourcetype.source_type as source_type, sourcing_candidatesourcetype.category as category, ')
    print(query)
    candidate_list = candidates_query.select(sql=query)
    return candidate_list, designation_list, locations_list, tags_list, status_list, added_by_list, source_list
