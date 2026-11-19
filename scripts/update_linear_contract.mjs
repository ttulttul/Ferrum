// Regenerate with node scripts/update_linear_contract.mjs /path/to/t3code after installing its dependencies.
import { readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";

const upstream = resolve(process.argv[2]);
const root = join(upstream, "packages/issue-tracker-linear");
const require = createRequire(join(root, "package.json"));
const generatorRequire = createRequire(require.resolve("@effect/graphql-generator/package.json"));
const gql = generatorRequire("graphql");
const schema = gql.buildSchema(readFileSync(join(root, "schema/linear.graphql"), "utf8"));
const document = gql.parse(readFileSync(join(root, "src/server/LinearApi.graphql"), "utf8"));
const fragments = new Map(document.definitions.filter(d => d.kind === "FragmentDefinition").map(d => [d.name.value,d]));

function fields(type, selection, used) {
  const all = {};
  function add(set) {
    for (const field of set.selections) {
      if (field.kind === "FragmentSpread") {
        const fragment = fragments.get(field.name.value);
        used.add(fragment);
        add(fragment.selectionSet);
      } else {
        const key = field.name.value;
        const list = all[key] ??= [];
        if (field.selectionSet) list.push(...field.selectionSet.selections);
      }
    }
  }
  add(selection);
  function shape(type, children) {
    if (!gql.isNonNullType(type)) return {nullable: value(type, children)};
    return value(type.ofType, children);
  }
  function value(type, children) {
    if (gql.isListType(type)) return [shape(type.ofType, children)];
    if (children.length) return fields(type, {selections:children}, used);
    if (gql.isEnumType(type)) return {enum:type.getValues().map(v=>v.name)};
    return type.name;
  }
  return Object.fromEntries(Object.entries(all).map(([key,children]) => [key,shape(type.getFields()[key].type,children)]));
}
const contracts = {};
for (const op of document.definitions.filter(d => d.kind === "OperationDefinition")) {
  const used = new Set();
  const result = fields(op.operation === "query" ? schema.getQueryType() : schema.getMutationType(), op.selectionSet, used);
  const query = gql.stripIgnoredCharacters(gql.print({kind:"Document",definitions:[op, ...used]}));
  contracts[op.name.value] = {query,result};
}
const path=fileURLToPath(new URL("../src/issues/graphql/linear.json", import.meta.url));
writeFileSync(path,"{\n"+Object.entries(contracts).map(([key,v])=>"  "+JSON.stringify(key)+": "+JSON.stringify(v)).join(",\n")+"\n}\n");
