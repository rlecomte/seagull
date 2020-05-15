extern crate nom;

use nom::{
    branch::alt, bytes::complete::tag, bytes::complete::tag_no_case,
    character::complete::multispace0, character::complete::multispace1,
    character::complete::none_of, multi::fold_many0, multi::fold_many1, multi::separated_list,
    IResult,
};

//
//  SELECT FORWARD FROM $ce-order("order-created") AT POSITON START
//
//
//  SELECT selector, [selector]
//  FROM ${stream-name}([event-type])
//  [WHERE selector = value [AND | OR selector = value]]

#[derive(Debug)]
struct Query {
    direction: String,
    stream_name: String,
    events: Vec<String>,
}

fn main() {
    let (r, q) = parse_query("SELECT FORWARD FROM $ce-order()").unwrap();
    println!("{} {:?}", r, q)
}

fn parse_query(input: &str) -> IResult<&str, Query> {
    let (r1, _) = parse_select(input)?;
    let (r2, d) = parse_direction(r1)?;
    let (r3, _) = parse_from(r2)?;
    let (r4, sn) = parse_stream_name(r3)?;
    let (r5, raw_evts) = parse_raw_events_content(r4)?;

    //FIXME unwrap
    let (_, evts) = parse_events(raw_evts.as_str()).unwrap();

    let q = Query {
        direction: d.to_string(),
        stream_name: sn,
        events: evts.iter().map(|s| s.to_string()).collect(),
    };

    Ok((r5, q))
}

fn parse_select(input: &str) -> IResult<&str, ()> {
    let (a, _) = multispace0(input)?;
    let (b, _) = tag_no_case("SELECT")(a)?;
    Ok((b, ()))
}

fn parse_direction(input: &str) -> IResult<&str, &str> {
    let (a, _) = multispace1(input)?;
    let (b, direction) = alt((tag_no_case("FORWARD"), tag_no_case("BACKWARD")))(a)?;
    Ok((b, direction))
}

fn parse_from(input: &str) -> IResult<&str, ()> {
    let (a, _) = multispace0(input)?;
    let (b, _) = tag_no_case("FROM")(a)?;
    Ok((b, ()))
}

fn parse_stream_name(input: &str) -> IResult<&str, String> {
    let (a, _) = multispace1(input)?;
    let (b, stream_name) = fold_many1(none_of("( \r\n\t("), String::new(), |mut acc, item| {
        acc.push(item);
        acc
    })(a)?;
    Ok((b, stream_name))
}

fn parse_raw_events_content(input: &str) -> IResult<&str, String> {
    let content_parser = fold_many0(none_of(")"), String::new(), |mut acc, item| {
        acc.push(item);
        acc
    });

    let (a, _) = multispace0(input)?;

    if a.is_empty() {
        Ok(("", String::from("")))
    } else {
        let (b, _) = tag("(")(a)?;
        let (c, raw_events) = content_parser(b)?;
        let (d, _) = tag(")")(c)?;
        Ok((d, raw_events))
    }
}

fn parse_events(input: &str) -> IResult<&str, Vec<String>> {
    let event_parser = fold_many0(none_of(","), String::new(), |mut acc, item| {
        acc.push(item);
        acc
    });
    let sep_parser = |s| -> IResult<&str, ()> {
        let (a, _) = multispace0(s)?;
        let (b, _) = tag(",")(a)?;
        let (c, _) = multispace0(b)?;
        Ok((c, ()))
    };

    if input.is_empty() {
        Ok((input, vec![]))
    } else {
        separated_list(sep_parser, event_parser)(input)
    }
}
