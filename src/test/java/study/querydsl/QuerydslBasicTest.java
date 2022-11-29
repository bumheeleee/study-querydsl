package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static org.junit.jupiter.api.Assertions.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {
    @PersistenceContext
    EntityManager em;

    private JPAQueryFactory jpaQueryFactory;

    @BeforeEach
    public void before() {
        jpaQueryFactory = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");

        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL(){
        //member1 찾기
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertEquals(findMember.getUsername(),"member1");
    }

    @Test
    public void startQuerydsl(){
        //QMember m = new QMember("m");
        Member member1 = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertEquals(member1.getUsername(), "member1");
    }

    @Test
    public void search(){
        Member findMember = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10))
                )
                .fetchOne();

        assertEquals(findMember.getUsername(), "member1");
        assertEquals(findMember.getAge(), 10);
    }

    @Test
    public void searchParam(){
        Member findMember = jpaQueryFactory
                .select(member)
                .from(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        // ","를 사용하여 and 연산을 할 수 있다.
        assertEquals(findMember.getUsername(), "member1");
        assertEquals(findMember.getAge(), 10);
    }

    @Test
    public void fetchTypeTest(){
        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .fetch();

        Member fetchOne = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        Member fetchFirst = jpaQueryFactory
                .selectFrom(member)
                .fetchFirst();

        //쿼리가 총 2방 나감 -> 총 개수 세는 쿼리, member 찾는 쿼리
        QueryResults<Member> results = jpaQueryFactory
                .selectFrom(member)
                .fetchResults();

        long total = results.getTotal();
        System.out.println("total = " + total);

        System.out.println("-----------------------");

        List<Member> contents = results.getResults();
        for (Member content : contents) {
            System.out.println("content = " + content);
        }

        long count = jpaQueryFactory
                .selectFrom(member)
                .fetchCount();
        System.out.println("count = " + count);
    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순 (desc)
     * 2. 회원 이름 올림차순 (asc)
     * 단 2에서 회원이름이 없으면(null) 마지막에 출력(nulls last)
     * jpql : select m from Member m order by m.age desc and m.username asc nulls last
     */
    @Test
    public void sortTest(){
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> results = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = results.get(0);
        Member member6 = results.get(1);
        Member memberNull = results.get(2);

        assertEquals(member5.getUsername(), "member5");
        assertEquals(member6.getUsername(), "member6");
        assertEquals(memberNull.getUsername(), null);
    }
    
    @Test
    public void pageTest1(){
        List<Member> results = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.asc())
                .offset(0)      // offset : 시작지점
                .limit(2)       // limit :  페이지 사이즈
                .fetch();

        for (Member result : results) {
            System.out.println("result.getUsername() = " + result.getUsername());
        }
        assertEquals(results.size(), 2);
        assertEquals(results.get(0).getUsername(), "member1");
        assertEquals(results.get(1).getUsername(), "member2");
    }

    @Test
    public void pageTest2(){
        QueryResults<Member> results = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.asc())
                .offset(0)      // offset : 시작지점
                .limit(2)       // limit :  페이지 사이즈
                .fetchResults();

        List<Member> contents = results.getResults();
        long total = results.getTotal();

        assertEquals(contents.size(), 2);
        assertEquals(total,4);
    }

    /**
     * 집합함수의 총 모음
     */
    @Test
    public void aggregationTest(){
        //데이터 타입이 이렇게 여러개인경우 -> tuple 사용!
        List<Tuple> results = jpaQueryFactory
                .select(member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();

        Tuple result = results.get(0);
        System.out.println("result.get(member.count()) = " + result.get(member.count()));
        System.out.println("result.get(member.age.sum()) = " + result.get(member.age.sum()));
        System.out.println("result.get(member.age.avg()) = " + result.get(member.age.avg()));
        System.out.println("result.get(member.age.max()) = " + result.get(member.age.max()));
        System.out.println("result.get(member.age.min()) = " + result.get(member.age.min()));
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구하기
     */
    @Test
    void group() {
        // given
        List<Tuple> result = jpaQueryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        // when
        Tuple team1 = result.get(0);
        Tuple team2 = result.get(1);

        System.out.println("result = " + result);
        // then
        System.out.println("team1 = " + team1);
        System.out.println("team2 = " + team2);
    }

    /**
     * 팀 A에 소속된 모든 회원을 조회
     * join() , innerJoin() : 내부 조인(inner join)
     * leftJoin() : left 외부 조인(left outer join)
     * rightJoin() : rigth 외부 조인(rigth outer join)
     */
    @Test
    void joinTest() {
        // given
        List<Member> members = jpaQueryFactory
                .select(member)
                .from(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .groupBy(member.username)
                .fetch();

        // when
        Member member1 = members.get(0);
        Member member2 = members.get(1);

        // then
        System.out.println("members = " + members);
        assertEquals(members.size(), 2);
        assertEquals(member1.getUsername(), "member1");
        assertEquals(member2.getUsername(), "member2");

        Assertions.assertThat(members)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인 (연관관계가 전혀 없는데도 조인이 가능하다.)
     * 회원의 이름과 팀의 이름이 같은 회원 조회
     */
    @Test
    void thetaJoin() {
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        // when
        //모든 member 테이블과 모든 team 테이블의 데이터를 조인한다. 그다음 where 조건
        List<Member> result = jpaQueryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        // then
        assertEquals(result.size(), 2);
        Assertions.assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }
    }

    /**
     * 예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * jpql: select m, t from Member m left join m.team t on t.name = "teamA"
     */
    @Test
    void joinOnFiltering() {
        // given
        //on 조건으로 하게되면, leftJoin 영향을 고대로 받아서 null 값도 고대로 출력한다.
        List<Tuple> outerOnResults = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();

        //where 조건으로 하게되면, where 조건으로 일치하는 것만 가져온다. (null 조건은 안가져온다.)
        List<Tuple> outerWhereResults = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        //inner join을 하게되면 이미 null값은 버려서 where에 조건을 주거나, on에 조건을 주는것은 동일하다.
        List<Tuple> innerJoinResults = jpaQueryFactory
                .select(member, team)
                .from(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        // when
        for (Tuple outerOnResult : outerOnResults) {
            System.out.println("outerOnResult = " + outerOnResult);
        }
        for (Tuple outerWhereResult : outerWhereResults) {
            System.out.println("outerWhereResult = " + outerWhereResult);
        }
        for (Tuple innerJoinResult : innerJoinResults) {
            System.out.println("innerJoinResult = " + innerJoinResult);
        }
    }

    /**
     * 연관관계가 없는 엔티티 외부조인(막 조인)
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     */
    @Test
    void joinOnNoRelation() {
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        // when
        // leftJoin -> on 절에 걸어서 null 조건도 다 출력된다
        List<Tuple> result = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        // then
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    void fetchJoinNo() {
        em.flush();
        em.clear();

        // given
        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        // when
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertEquals(loaded, false);
    }

    @Test
    void fetchJoinUse() {
        em.flush();
        em.clear();

        // given
        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        // when
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertEquals(loaded, true);
    }

    /**
     * 서브 쿼리
     * com.querydsl.jpa.JPAExpressions 사용
     * 나이가 가장 많은 회원 조회
     */
    @Test
    void subQueryTest() {
        QMember memberSub = new QMember("memberSub");

        Member findMember = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetchOne();

        System.out.println("findMember = " + findMember);
    }

    /**
     * 서브 쿼리
     * 나이가 평균 이상인 회원
     */
    @Test
    void subQueryTest2() {
        QMember memberSub = new QMember("memberSub");

        List<Member> members = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertEquals(members.size(), 2);
        for (Member member1 : members) {
            System.out.println("member1 = " + member1);
        }
    }

    /**
     * 서브 쿼리
     * 여러 건 처리 , in 사용
     */
    @Test
    void subQueryTest3() {
        QMember memberSub = new QMember("memberSub");

        List<Member> members = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.in(
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        assertEquals(members.size(), 3);
        for (Member member1 : members) {
            System.out.println("member1 = " + member1);
        }
    }

    /**
     * select 절에서의 subQuery
     */
    @Test
    void selectSubQuery() {
        // given
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = jpaQueryFactory
                .select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub)
                ).from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    void basicCaseTest() {
        // given
        List<String> results = jpaQueryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        // then
        for (String result : results) {
            System.out.println("result = " + result);
        }
    }

    @Test
    void complexCaseTest() {
        // given
        List<String> results = jpaQueryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(10, 20)).then("10~20살")
                        .when(member.age.between(20, 30)).then("20~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        // then
        for (String result : results) {
            System.out.println("result = " + result);
        }
    }

    @Test
    void constantPlusTest() {
        // given
        List<Tuple> results = jpaQueryFactory
                .select(member.age, Expressions.constant("lee"))
                .from(member)
                .fetch();

        // then
        for (Tuple result : results) {
            System.out.println("result = " + result);
        }
    }

    @Test
    void concatTest() {
        // given
        // {username}_{age}
        List<String> results = jpaQueryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .fetch();

        // then
        for (String result : results) {
            System.out.println("result = " + result);
        }
    }
}
