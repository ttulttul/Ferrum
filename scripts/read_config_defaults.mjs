// Export pure upstream defaults. No CLI discovery or account probes run here.
import fs from "node:fs";
import path from "node:path";
import {pathToFileURL} from "node:url";
import {createRequire} from "node:module";
const root=path.resolve(process.argv[2]);
const load=(p)=>import(pathToFileURL(path.join(root,p)));
const eff=await import(pathToFileURL(createRequire(path.join(root,"apps/server/package.json")).resolve("effect/Effect")));
const {DEFAULT_KEYBINDINGS}=await load("packages/shared/src/keybindings.ts");
const {STATIC_KEYBINDING_COMMANDS}=await load("packages/contracts/src/keybindings.ts");
const {EDITORS}=await load("packages/contracts/src/editor.ts");
const definitions=[
 ["codex","apps/server/src/provider/CodexProvider.ts","makePendingCodexProvider"],
 ["claudeAgent","apps/server/src/provider/ClaudeProvider.ts","makePendingClaudeProvider"],
 ["cursor","packages/provider-cursor/src/server/status.ts","buildInitialCursorProviderSnapshot"],
 ["grok","packages/provider-grok/src/server/status.ts","buildInitialGrokProviderSnapshot"],
 ["opencode","packages/provider-opencode/src/server/status.ts","makePendingOpenCodeProvider"],
 ["antigravity",null,null],
 ["fx","packages/provider-fx/src/server/status.ts","buildInitialFxProviderSnapshot"],
 ["kiro","packages/provider-kiro/src/server/status.ts","buildInitialKiroProviderSnapshot"],
 ["muse","packages/provider-muse/src/server/status.ts","makePendingMuseProvider"],
 ["pi","packages/provider-pi/src/server/status.ts","buildInitialPiProviderSnapshot"]
];
const providers={};
for(const [id,file,func] of definitions){
  if(!file){
    providers[id]={displayName:"Antigravity",showInteractionModeToggle:false,supportsConversationRollback:true,supportsTextGeneration:false,supportedRuntimeModes:["approval-required","auto-accept-edits","full-access"],
      models:[],slashCommands:[],skills:[],setup:{canAuthenticate:true,canInstall:true}};
    continue;
  }
  const initial=(await load(file))[func];
  const snapshot=await eff.runPromise(initial({enabled:false,customModels:[],binaryPath:"",homePath:"",serverUrl:""}));
  const catalog={};
  for(const key of ["displayName","showInteractionModeToggle","reportsContextWindow","supportedRuntimeModes","supportsConversationRollback","supportsTextGeneration","models","slashCommands","skills"]){
    if(snapshot[key]!==undefined)catalog[key]=snapshot[key];
  }
  catalog.setup={canAuthenticate:id==="cursor",canInstall:false};
  providers[id]=catalog;
}
const defaults={keybindings:DEFAULT_KEYBINDINGS,commands:STATIC_KEYBINDING_COMMANDS,editors:EDITORS.map(({id,commands})=>({id,commands})),providers};
fs.writeFileSync(process.argv[3],JSON.stringify(defaults,null,2)+"\n");
