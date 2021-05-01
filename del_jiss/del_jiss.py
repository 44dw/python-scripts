import requests
import argparse
import random
import sys
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

GET_ISSUES_URL = 'http://sbtatlas.sigma.sbrf.ru/jira/rest/api/2/search'
DELETE_ISSUE_URL = 'http://sbtatlas.sigma.sbrf.ru/jira/rest/api/2/issue/'
GET_ISSUES_PARAMS = {
    'jql': 'reporter=out-akhmetshin-da and resolution is empty and project=JENFLOWCTL',
    'fields': 'summary',
    'maxResults': '1000'
}
POSSIBLE_SUMMARIES = ['Создать скрипты', 'Предоставить доступ к ', 'Удалить группы', 'Добавить секрет для od-dev',
                      'Сброс пароля ', 'Ошибка на Портале самообслуживания']
TRIMMED_LIST_LENGTH = 10


def get_jira_issues():
    def filter_by_summary(issue):
        summary = issue['fields']['summary']
        for sum in POSSIBLE_SUMMARIES:
            if summary.find(sum) > -1:
                return True
        return False



    response = requests.get(GET_ISSUES_URL,
                            params=GET_ISSUES_PARAMS,
                            headers={'Content-Type': 'application/json'},
                            auth=('', ''),
                            verify=False)
    body = response.json()
    filtered = list(filter(filter_by_summary, body['issues']))
    issues = list(map(lambda issue: issue['key'], filtered))
    print('next issues found: ', issues)
    return issues

def delete_issue(issue):
    print("deleting issue %s..." % issue)
    response = requests.delete(DELETE_ISSUE_URL + issue,
                               headers={'Content-Type': 'application/json'},
                               auth=('', ''),
                               verify=False)
    print('delete issue response:' + str(response.status_code))

def delete_issues(issues, to_delete_all):
    def delete_all_issues(issues):
        [delete_issue(issue) for issue in issues]

    def delete_some_issues(issues):
        random.shuffle(issues)
        some_issues = issues[:TRIMMED_LIST_LENGTH]
        print('issues to delete:', some_issues)
        [delete_issue(issue) for issue in some_issues]


    if to_delete_all:
        delete_all_issues(issues)
    else:
        delete_some_issues(issues)




if __name__ == '__main__':
    print(sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", help="pass single issue to delete", type=str)
    parser.add_argument("-a", help="to delete all issues", action="store_true")
    args = parser.parse_args()
    if args.i:
        print('issue to delete: ' + str(args.i))
        delete_issue(args.i)
    else:
        print('deleting all: ' + str(args.a))
        issues = get_jira_issues()
        delete_issues(issues, args.a)


