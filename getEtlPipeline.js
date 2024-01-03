// [POST] /api/etl-pipeline
exports.getEtlPipeline = async (req, res) => {
  const {
    collectionName,
    procName,
    schemaName,
    procType,
    blobDelim,
    procData,
  } = req.body;

  const singleValues = procData.filter(
    (procItem) => procItem["should_parse_sv"]
  );
  const vms = procData.filter((procItem) => procItem["should_parse_vm"]);
  const vss = procData.filter((procItem) => procItem["should_parse_vs"]);

  let stmtRaw = await services.getTemplateByName(collectionName, "RAW");
  let stmtMapped = await services.getTemplateByName(collectionName, procType);
  let stmtMultival;
  let stmtSink;
  let stmtDdl;

  let sourceStream;
  let selectedFields;
  let listSelectedField;
  let vm;
  let vs;

  const singleHandler = ({ name, transformation, type, nested }) => {
    let output;
    let fieldName = name.startsWith("LOCALREF_")
      ? name.split("LOCALREF_")[1]
      : name;
    if (name === "INPUTTER_HIS") {
      output = `SUBSTRING(REGEXP_REPLACE(ARRAY_JOIN(TRANSFORM(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['INPUTTER_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]*:)?'),x => SEAB_FIELD(x,'_',2)),' '),'null ',''),1,4000)`;
      fieldName = "INPUTTER_HIS";
    } else if (transformation === "") {
      output = `DATA.XMLRECORD['${name}']`;
    } else if (transformation.includes("string-join")) {
      const pattern = /\('*([^']*)'*\)$/;
      if (pattern.test(transformation)) {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
          transformation.match(pattern)[1]
        }')`;
      } else {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
      }
    } else if (transformation == "parse_date") {
      output = `PARSE_DATE(DATA.XMLRECORD['${name}'], 'yyyyMMdd')`;
    } else if (transformation == "parse_timestamp") {
      output = `PARSE_TIMESTAMP(DATA.XMLRECORD['${name}'], 'yyMMddHHmm')`;
    } else if (transformation == "substring") {
      output = `SUBSTRING(DATA.XMLRECORD['${name}'],1,35)`;
    } else if (transformation === "seab_field") {
      output = `SEAB_FIELD(DATA.XMLRECORD['${name}'],'_',2)`;
    } else if (/^\[(.*)\]$/.test(transformation)) {
      output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
        transformation.match(/^\[(.*)\]$/)[1]
      }]`;
    } else if (/^([^\s]*)\((.*)\)\s*(.*)$/.test(transformation)) {
      const matches = transformation.match(/^([^\s]*)\((.*)\)\s*(.*)$/);

      let field = `DATA.XMLRECORD['${name}']`;

      fieldName = matches[3];
      matches[1] = matches[1].toUpperCase();
      if (/^\$/.test(matches[2])) {
        if (name === "RECID") {
          field = "DATA.RECID";
        } else if (transformation.includes("string-join")) {
          field = `DATA.XMLRECORD['${name}_multivalue']`;
        }

        output = `${matches[1]}(${matches[2].replace("$", field)})`;
      } else if (/^\[.*\](.*)$/.test(matches[2])) {
        const matches2 = matches[2].match(/^\[(.*)\](.*)$/);

        let field = `DATA.XMLRECORD['${name}_multivalue']`;
        let params;

        if (transformation.includes("parse_date")) {
          params = `, 'yyyyMMdd'`;
        } else if (transformation.includes("parse_timestamp")) {
          params = `, 'yyMMddHHmm'`;
        } else if (transformation.includes("substring")) {
          params = `,1,35`;
        } else if (transformation.includes("seab_field")) {
          params = `,'_',2`;
        }

        if (name === "RECID") {
          field = "DATA.RECID";
        }

        if (/[^,\s]/.test(matches2[2])) {
          params = matches2[2];
        }

        output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(${field}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches2[1]}]${params})`;
      }
    } else {
      return `\t${transformation}`;
    }
    output = nested.includes("$") ? nested.replace("$", output) : output;
    if (type[1] !== "string") {
      output = `CAST(${output} AS ${type[1]})`;
    }
    return `\t${output} AS ${fieldName.toUpperCase() || name} ,`;
  };

  const multiHandler = ({ name, transformation, type, nested }) => {
    let output;
    let fieldName = name.startsWith("LOCALREF_")
      ? name.split("LOCALREF_")[1]
      : name;
    if (name === "INPUTTER_HIS") {
      output = `SUBSTRING(REGEXP_REPLACE(ARRAY_JOIN(TRANSFORM(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['INPUTTER'],'^s?[0-9]+:',''), '#(s?[0-9]*:)?'),x => SEAB_FIELD(x,'_',2)),' '),'null ',''),1,4000)`;
      fieldName = "INPUTTER_HIS";
    } else if (transformation === "") {
      output = `DATA.XMLRECORD['${name}']`;
    } else if (transformation.includes("string-join")) {
      const pattern = /\('*([^']*)'*\)$/;
      if (pattern.test(transformation)) {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
          transformation.match(pattern)[1]
        }')`;
      } else {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
      }
    } else if (transformation == "parse_date") {
      output = `PARSE_DATE(DATA.XMLRECORD['${name}'], 'yyyyMMdd')`;
    } else if (transformation == "parse_timestamp") {
      output = `PARSE_TIMESTAMP(DATA.XMLRECORD['${name}'], 'yyMMddHHmm')`;
    } else if (transformation == "substring") {
      output = `SUBSTRING(DATA.XMLRECORD['${name}'],1,35)`;
    } else if (transformation === "seab_field") {
      output = `SEAB_FIELD(DATA.XMLRECORD['${name}'],'_',2)`;
    } else if (/^\[(.*)\]$/.test(transformation)) {
      output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
        transformation.match(/^\[(.*)\]$/)[1]
      }]`;
    } else if (/^([^\s]*)\((.*)\)\s*(.*)$/.test(transformation)) {
      const matches = transformation.match(/^([^\s]*)\((.*)\)\s*(.*)$/);
      fieldName = matches[3];
      matches[1] = matches[1].toUpperCase();
      if (/^\$/.test(matches[2])) {
        if (name === "RECID") {
          output = `${matches[1]}(${matches[2].replace("$", `DATA.RECID`)})`;
        } else {
          output = `${matches[1]}(${matches[2].replace(
            "$",
            `DATA.XMLRECORD['${name}']`
          )})`;
        }
        fieldName = matches[3];
      } else if (/^\[.*\](.*)$/.test(matches[2])) {
        const matches2 = matches[2].match(/^\[(.*)\](.*)$/);

        let field = `DATA.XMLRECORD['${name}']`;
        let params;

        if (transformation.includes("parse_date")) {
          params = `, 'yyyyMMdd'`;
        } else if (transformation.includes("parse_timestamp")) {
          params = `, 'yyMMddHHmm'`;
        } else if (transformation.includes("substring")) {
          params = `,1,35`;
        } else if (transformation.includes("seab_field")) {
          params = `,'_',2`;
        }

        if (name === "RECID") {
          field = "DATA.RECID";
        }

        if (/[^,\s]/.test(matches2[2])) {
          params = matches2[2];
        }

        output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(${field}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches2[1]}]${params})`;
      }
    } else {
      return `\t${transformation}`;
    }
    output = nested.includes("$") ? nested.replace("$", output) : output;
    if (type[1] !== "string") {
      output = `CAST(${output} AS ${type[1]})`;
    }
    return `\t${output} AS ${fieldName.toUpperCase() || name},`;
  };

  if (vms.length || vss.length) {
    sourceStream = `${schemaName}_MULTIVALUE`;
    stmtSink = await services.getTemplateByName(
      collectionName,
      "SINK_MULTIVALUE"
    );
    stmtMultival = await services.getTemplateByName(
      collectionName,
      "MULTIVALUE"
    );

    listSelectedField = singleValues
      .map(({ name, transformation }) => {
        let output = `DATA.XMLRECORD['${name}']`;
        let fieldName = name.startsWith("LOCALREF_")
          ? name.split("LOCALREF_")[1]
          : name;
        if (/(.*\(.*\))\s([^,]*),*$/.test(transformation)) {
          const matches = transformation.match(/(.*\(.*\))\s([^,]*),*$/);
          fieldName = matches[2];
        } else if (
          transformation.includes("string-join") ||
          /^\[(.*)\]$/.test(transformation) ||
          /(.*)\(\[(.*)\](.*)\)/.test(transformation)
        ) {
          output = `DATA.XMLRECORD['${name}_multivalue']`;
        }
        return `\t${output} AS ${fieldName.toUpperCase() || name},`;
      })
      .join("\n");

    vm = vms.map(({ name }) => `'${name}'`).join(", ") || `''`;
    vs = vss.map(({ name }) => `'${name}'`).join(", ") || `''`;

    selectedSingle = singleValues.map(
      ({ name, transformation, type, nested }) => {
        let output;
        let fieldName = name.startsWith("LOCALREF_")
          ? name.split("LOCALREF_")[1]
          : name;
        if (name === "INPUTTER_HIS") {
          output = `SUBSTRING(REGEXP_REPLACE(ARRAY_JOIN(TRANSFORM(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.INPUTTER,'^s?[0-9]+:',''), '#(s?[0-9]*:)?'),x => SEAB_FIELD(x,'_',2)),' '),'null ',''),1,4000)`;
          fieldName = "INPUTTER_HIS";
        } else if (transformation === "") {
          output = `DATA.${name}`;
        } else if (transformation.includes("string-join")) {
          const pattern = /\('*([^']*)'*\)$/;
          if (pattern.test(transformation)) {
            output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.${name},'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
              transformation.match(pattern)[1]
            }')`;
          } else {
            output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.${name},'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
          }
        } else if (transformation == "parse_date") {
          output = `PARSE_DATE(DATA.${name}, 'yyyyMMdd')`;
        } else if (transformation == "parse_timestamp") {
          output = `PARSE_TIMESTAMP(DATA.${name}, 'yyMMddHHmm')`;
        } else if (transformation == "substring") {
          output = `SUBSTRING(DATA.${name},1,35)`;
        } else if (transformation === "seab_field") {
          output = `SEAB_FIELD(DATA.${name},'_',2)`;
        } else if (/^\[(.*)\]$/.test(transformation)) {
          output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.${name}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
            transformation.match(/^\[(.*)\]$/)[1]
          }]`;
        } else if (/^([^\s]*)\((.*)\)\s*(.*)$/.test(transformation)) {
          const matches = transformation.match(/^([^\s]*)\((.*)\)\s*(.*)$/);
          fieldName = matches[3];
          matches[1] = matches[1].toUpperCase();
          if (/^\$/.test(matches[2])) {
            if (name === "RECID") {
              output = `${matches[1]}(${matches[2].replace(
                "$",
                `DATA.RECID`
              )})`;
            } else {
              output = `${matches[1]}(${matches[2].replace(
                "$",
                `DATA.${name}`
              )})`;
            }
            fieldName = matches[3];
          } else if (/^\[.*\](.*)$/.test(matches[2])) {
            const matches2 = matches[2].match(/^\[(.*)\](.*)$/);

            let field = `DATA.${name}`;
            let params;

            if (transformation.includes("parse_date")) {
              params = `, 'yyyyMMdd'`;
            } else if (transformation.includes("parse_timestamp")) {
              params = `, 'yyMMddHHmm'`;
            } else if (transformation.includes("substring")) {
              params = `,1,35`;
            } else if (transformation.includes("seab_field")) {
              params = `,'_',2`;
            }
            if (name === "RECID") {
              field = "DATA.RECID";
            }

            if (/[^,\s]/.test(matches2[2])) {
              params = matches2[2];
            }
            output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(${field}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches2[1]}]${params})`;
          }
        } else {
          return `\t${transformation}`;
        }
        output = nested.includes("$") ? nested.replace("$", output) : output;
        if (type[1] !== "string") {
          output = `CAST(${output} AS ${type[1]})`;
        }
        return `\t${output} AS ${fieldName.toUpperCase() || name},`;
      }
    );
    selectedMulti = vms.map(multiHandler);
    selectedVS = vss.map(multiHandler);
    selectedFields = selectedSingle
      .concat(selectedMulti)
      .concat(selectedVS)
      .join("\n");
    stmtDdl = await services.getTemplateByName(
      collectionName,
      "DDL_MULTIVALUE"
    );
  } else {
    sourceStream = `${schemaName}_MAPPED`;
    stmtSink = await services.getTemplateByName(collectionName, "SINK");
    selectedFields = singleValues.map(singleHandler).join("\n");
    stmtDdl = await services.getTemplateByName(collectionName, "DDL_SINGLE");
  }

  stmtRaw = eval("`" + stmtRaw + "`");
  stmtMapped = eval("`" + stmtMapped + "`");
  stmtMultival = stmtMultival && eval("`" + stmtMultival + "`");
  stmtSink = eval("`" + stmtSink + "`");
  stmtDdl = eval("`" + stmtDdl + "`");
  res.status(200).send({
    stmtRaw,
    stmtMapped,
    stmtMultival,
    stmtSink,
    stmtDdl,
  });
};
