import { Component } from 'react';
import type { ReactNode } from 'react';
import { StateMessage } from './Controls';

export default class VisualBoundary extends Component<{ children: ReactNode; name: string; resetKey: string }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }
  componentDidUpdate(previous: Readonly<{ resetKey: string }>) {
    if (previous.resetKey !== this.props.resetKey && this.state.failed) this.setState({ failed: false });
  }
  render() {
    return this.state.failed ? <StateMessage title={`Could not render the ${this.props.name}`} description="The other parts of the explorer are still available."
      onRetry={() => this.setState({ failed: false })} /> : this.props.children;
  }
}
