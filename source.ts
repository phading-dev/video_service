import { EnumDescriptor } from '@selfage/message/descriptor';

export enum Source {
  SHOW = 1,
}

export let SOURCE: EnumDescriptor<Source> = {
  name: 'Source',
  values: [{
    name: 'SHOW',
    value: 1,
  }]
}
