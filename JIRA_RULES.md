# JIRA_RULES.md (Voice → Jira)

목표: 전사 텍스트에서 Jira 태스크 후보를 만들고, **승인 후에만** Jira 이슈를 생성한다.

## 0) 기본값(합리적 디폴트)

- 기본 프로젝트 키: `JIRA_PROJECT_KEY` (env)
- 기본 이슈 타입: `Task`
- 우선순위 기본: `Medium` (인스턴스에 따라 이름이 다를 수 있으니 설정으로 오버라이드 가능)
- 라벨 기본: `voice`, 추가로 `meeting`(회의/미팅 키워드 포함 시)

## 1) 후보 생성 규칙

- 후보는 최대 10개
- 각 후보는 다음 필드를 가진다:
  - `summary` (<= 120 chars)
  - `description` (맥락 + 결정사항 + 다음 액션)
  - `labels` (기본 + 상황)
  - `issueType` (기본 Task)
  - `priority` (optional)
  - `assigneeAccountId` (optional)

## 2) Description 템플릿(권장)

```
## Context
- Source: Discord voice / audio
- Run: <run_id>
- Date: <YYYY-MM-DD HH:mm KST>

## Summary
<1~4줄 요약>

## Action Items
- [ ] ...

## Notes (masked)
- ...
```

## 3) 민감정보 마스킹

- EMAIL/PHONE/PNR/여권/계정 토큰/주소/결제정보는 출력 및 Jira description에 **마스킹 후** 기록
- 마스킹 기본:
  - email: `a***@domain.com`
  - phone: `010-12**-****`
  - PNR/예약번호: `AB***12`

## 4) 승인 게이트(필수)

- 후보 생성까지만 자동
- Jira 이슈 생성은 사용자가 아래 중 하나로 명시 승인해야 함:
  - `승인: Jira 생성 (run_id=..., 1,3,5)`
  - 또는 `approve jira <run_id> 1,3,5`

## 5) idempotency(중복 생성 방지)

- **idempotency_key** 정의(권장):

`sha256(projectKey + "|" + issueType + "|" + normalize(summary) + "|" + transcript_hash + "|" + candidate_index)`

- 생성 스크립트는 로컬 로그(`logs/jira/idempotency.jsonl`)에 idempotency_key를 저장
- 동일 key로 재실행 시:
  - 새 이슈 생성 금지
  - 기존 `issue_key/issue_url` 반환

## 6) Jira 계정/권한

- Jira Cloud REST API는 assignee에 `accountId`를 사용
- API Token은 최소 권한 원칙(프로젝트 범위)으로 발급

---
참고
- Jira Cloud REST API (Create issue): https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-post
- Atlassian Document Format(ADF): https://developer.atlassian.com/cloud/jira/platform/apis/document/structure/
